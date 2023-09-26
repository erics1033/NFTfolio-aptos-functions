import * as admin from 'firebase-admin';
import {
  consoleLog,
  convertStatsToEth,
  createCollectionInstance,
  createFirestoreBatchInstance,
  errorLog,
  executeWriteBatch,
  getMultipleCoinData,
  performStatsCalcFromStatsYesterday,
  timeStamp,
} from '../../../service';
import { COLLECTION_NAMES, SUPPORTED_CHAINS } from '../../../values';

/**
 * 1) Loops through all Aptos collections in top_collections (where caught_up_txn=true)
 * 2) Checks to see if each collection has any active listings and/or 24h sales activity
 * 3) Uses the active listings and 24h sales activity to update stats
 * including floor_price, usd_floor_price, one_day_volume, average_price,
 * one_day_sales, listed_count, market_cap, stats_eth equivalents
 *
 * @return {any} success: true/false, error
 */
export const updateAptosCollectionStats = async () => {
  try {
    const updateBatch = createFirestoreBatchInstance();

    const queryData: any = [];

    const topCollectionRef: any = createCollectionInstance(
      COLLECTION_NAMES.TOP_COLLECTIONS
    );

    // NOTE: max of 500 collection until switch to array for updateBatch
    const queryResult: any = await topCollectionRef
      .where('active', '==', true)
      .where('chain', '==', 'aptos')
      .orderBy(admin.firestore.FieldPath.documentId(), 'desc')
      .limit(500)
      .get();

    await queryResult.forEach(async (doc: any) => {
      const id = doc.id;
      const data = doc.data();

      queryData.push({ ...data, id });
    });

    // Needed to calculate usd_floor_price and stats_eth
    const ethConversion: any = {};
    const fiatConversion: any = {};

    const coinResponse = await getMultipleCoinData(SUPPORTED_CHAINS);
    if (coinResponse) {
      Object.keys(coinResponse).map((key) => {
        const currentPrice = coinResponse[key]['market_data']['current_price'];
        if (currentPrice['eth']) {
          ethConversion[key] = currentPrice['eth'];
        }
        if (currentPrice['usd']) fiatConversion[key] = currentPrice['usd'];
      });
    }

    consoleLog('ethConversion', ethConversion);
    consoleLog('fiatConversion', fiatConversion);

    for (let i = 0; i < queryData.length; i++) {
      let collectionData = queryData[i];

      // Skip over collections where not caught up yet onchain
      if (collectionData?.caught_up_txn_version == false) continue;

      let getUpdatedStats = await getAptosUpdatedStats(
        collectionData,
        fiatConversion
      );

      if (getUpdatedStats) {
        const ref = createCollectionInstance(
          COLLECTION_NAMES.TOP_COLLECTIONS
        ).doc(collectionData?.id);

        // Calculate the calculated_percentage_change field
        const calcResponse = await performStatsCalcFromStatsYesterday({
          activeCollection: collectionData,
          updatedStats: getUpdatedStats,
        });
        collectionData = calcResponse?.activeCollection;
        getUpdatedStats = calcResponse?.updatedStats;

        // Align eth values
        collectionData['stats_eth'] = convertStatsToEth(
          getUpdatedStats,
          collectionData['stats_eth'],
          ethConversion['aptos'],
          ['market_cap', 'floor_price', 'one_day_volume']
        );

        collectionData.last_updated_at = timeStamp();
        collectionData.stats = getUpdatedStats;

        updateBatch.set(ref, collectionData, { merge: true });
      }
    }

    await executeWriteBatch(
      updateBatch,
      'Update aptos col floor prices data batch'
    );
    return { success: true };
  } catch (error) {
    errorLog('updateAptosCollectionStats | Error ', error);
    return { success: false, error };
  }
};

// Gets a collection's active listings and returns updated stats
// 1) Gets a collection's lowest price active listing
// 2) Calculates: floor_price, usd_floor_price and listed_count
// 3) Fetches other stats from sales activity function
// 4) Returns updated collection stats
const getAptosUpdatedStats = (
  collection: any,
  fiatConversion: any
): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve) => {
    try {
      let aptosListingsRef: any = createCollectionInstance(
        COLLECTION_NAMES.APTOS_LISTINGS
      );

      aptosListingsRef = await aptosListingsRef
        .where('top_collection_id', '==', collection?.id)
        .orderBy('price', 'asc');

      const queryResult: any = await aptosListingsRef.get();

      consoleLog(
        `${collection?.name}, lowest listed price: 
        ${queryResult?.docs[0]?.data()?.price} APT`
      );

      const { one_day_volume, one_day_sales, average_price } =
        await getAptosDailyActivityStats(collection?.id);

      const usd_floor_price = (
        Number(queryResult?.docs[0]?.data()?.price || 0) *
        Number(fiatConversion['aptos'])
      ).toFixed(2);

      consoleLog('usd_floor_price', usd_floor_price);

      const newStats = {
        ...(collection?.stats || []),
        floor_price: queryResult?.docs[0]?.data()?.price || null,
        average_price: average_price || 0,
        one_day_average_price: average_price || 0,
        listed_count: queryResult?.size || 0,
        one_day_volume: one_day_volume || 0,
        one_day_sales: one_day_sales || 0,
        market_cap:
          (queryResult?.docs[0]?.data()?.price || 0) *
          (collection?.stats?.total_supply || 1),
        usd_floor_price: !isNaN(Number(usd_floor_price))
          ? Number(usd_floor_price)
          : 0,
      };

      resolve(newStats);
    } catch (error) {
      errorLog('getAptosUpdatedStats - error: ', error);
      resolve(null);
    }
  });
};

// 1) Uses top_collection_id to get all the daily activity stats
// for the collection
// 2) Calculates stats: one_day_volume, one_day_sales, average_price
// 3) Returns the stats
const getAptosDailyActivityStats = (
  top_collection_id: string
): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve) => {
    try {
      let aptosActivitiesRef: any = createCollectionInstance(
        COLLECTION_NAMES.APTOS_ACTIVITIES
      );

      aptosActivitiesRef = await aptosActivitiesRef
        .where('top_collection_id', '==', top_collection_id)
        .orderBy('block_datetime', 'desc')
        .limit(200);

      const queryResult: any = await aptosActivitiesRef.get();

      let oneDayVolume = 0;
      let oneDaySales = 0;
      let averagePrice = 0;

      // Create a new Date object representing the current date and time
      const currentDate = new Date();

      // Get the current Unix timestamp in milliseconds
      const unixTimestamp = currentDate.getTime();
      const unixTimestampInSeconds = Math.floor(unixTimestamp / 1000);

      // Calculate the number of milliseconds in 24 hours
      const twentyFourHoursInMilliseconds = 24 * 60 * 60 * 1000;

      if (queryResult?.size > 0) {
        for (let i = 0; i < queryResult?.docs?.length; i++) {
          const docData = queryResult.docs[i].data();

          // Calculate the time difference in milliseconds
          const timeDifference =
            (unixTimestampInSeconds - docData.block_datetime._seconds) * 1000;

          // Check if a sale activity happened within the past 24 hours
          if (timeDifference <= twentyFourHoursInMilliseconds) {
            oneDayVolume += docData?.price;
            oneDaySales += 1;
          }
        }
        averagePrice =
          oneDaySales > 0
            ? Number((oneDayVolume / oneDaySales)?.toFixed(2))
            : 0;
        oneDayVolume = Number(oneDayVolume?.toFixed(2));
      }

      resolve({
        one_day_volume: oneDayVolume,
        one_day_sales: oneDaySales,
        average_price: averagePrice,
      });
    } catch (error) {
      errorLog(
        `getAptosDailyActivityStats, ${top_collection_id} - error: `,
        error
      );
      resolve({
        one_day_volume: null,
        one_day_sales: null,
        average_price: null,
      });
    }
  });
};
