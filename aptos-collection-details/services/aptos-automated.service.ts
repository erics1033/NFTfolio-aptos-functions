import {
  consoleLog,
  createCollectionInstance,
  createFirestoreBatchInstance,
  errorLog,
  executeWriteBatch,
  timeStamp,
} from '../../../service';
import {
  APTOS_SUPPORTED_MARKETPLACES,
  COLLECTION_NAMES,
} from '../../../values';
import { uploadImageToS3 } from '../../aws-images/services';
import { executeAptosListingsBatchApiRequest } from './aptos-listings.service';
import {
  fetchLatestVolume,
  fetchMyQueryCurrentTokenDatas,
} from './graphql.service';
import { getNftImageFromMetadataUri } from './helper.service';

// 1) Gets oldest created APT collection that is not caught up on txn events
// to present.
// 2) Get collection's next 200 events and add listings/sales activity to db
// 3) If collection is caught up, change caught_up_txn to true
export const catchUpAptosCollections = (): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const updateBatch = createFirestoreBatchInstance();

      /** Top Collection Firestore Reference **/
      let topCollectionRef: any = createCollectionInstance(
        COLLECTION_NAMES.TOP_COLLECTIONS
      );

      /** Get top aptos collections not complete **/
      topCollectionRef = await topCollectionRef
        .where('active', '==', true)
        .where('chain', '==', 'aptos')
        .where('caught_up_txn_version', '==', false)
        .orderBy('created_at', 'asc');

      const queryData: any = [];

      const queryResult: any = await topCollectionRef.limit(1).get();

      // Get document data from query result
      await queryResult.forEach(async (doc: any) => {
        const id = doc.id;
        const data = doc.data();

        queryData.push({ ...data, id });
      });

      consoleLog(
        'catchUpAptosCollections - queryData.length: ',
        queryData.length
      );

      // Do max of 200 txns for the collection or until <200 on a transaction, set caught_up_txn=true
      // Add the activity and listings for that one
      await executeAptosListingsBatchApiRequest(queryData, updateBatch, true);

      // Execute update aptos collections batch
      await executeWriteBatch(
        updateBatch,
        'Catch up aptos collections data batch'
      );

      resolve({
        success: true,
      });
    } catch (error) {
      errorLog('fetchNewAptosCollectionsAutomated Error', error);
      reject(error);
    }
  });
};

// Fetches and adds new unsupported APT collections to app based on latest volume
// 1) Checks daily volume across 4 marketplaces and finds the top 5 collections by daily volume
// 2) Checks each collection by verified_creator_address to see if in top_collections
// 3) If it's not, adds the collection and adds field caught_up=false
export const fetchNewAptosCollectionsAutomated = (): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const newCollections: any = [];

      // 1) Checks daily volume across 4 marketplaces and finds the top 5 collections by daily volume
      const topFiveCollections: any =
        await getTopAptosCollectionsByDailyVolume();
      consoleLog('topFiveCollections - ', topFiveCollections);

      for (let i = 0; i < topFiveCollections.length; i++) {
        // 2) Checks each collection by verified_creator_address to see if in top_collections
        const colAlreadyExists =
          await getAptosCollectionByVerifiedCreatorAddress(
            topFiveCollections[i].verified_creator_address
          );

        // 3) If it's not, adds the collection and adds field caught_up=false
        if (!colAlreadyExists) {
          consoleLog(
            `${topFiveCollections[i].collection_name} col doesnt exist, generating to add to database...`
          );

          const collection = await generateAptosCollection(
            topFiveCollections[i].verified_creator_address,
            topFiveCollections[i].collection_name
          );

          if (collection?.stats?.total_supply > 0) {
            await createCollectionInstance(
              COLLECTION_NAMES.TOP_COLLECTIONS
            ).add(collection);

            newCollections.push(collection);
          } else {
            consoleLog(
              `Skipped adding collection: ${collection?.name} with 0 supply, verifiedCreatorAddr: ${collection?.verified_creator_address}`
            );
          }
        }
      }

      consoleLog(`Total new collections added: ${newCollections.length}`);
      resolve({
        success: true,
      });
    } catch (error) {
      errorLog('fetchNewAptosCollectionsAutomated Error', error);
      reject(error);
    }
  });
};

/**
 * Gets top 5 APT collections by daily volume
 * @return {any} top 5 collections by daily volume
 */
async function getTopAptosCollectionsByDailyVolume() {
  try {
    const latest_sales = await getLatestAptosVolume();
    consoleLog('latest_sales.length - ', latest_sales.length);

    let collections: any = [];

    // Loop through the sales list and calculates the sum of prices for each creator_address
    latest_sales.forEach((item: any) => {
      const { collection_name, price, creator_address } = item;

      // Check if collection exists in an array by matching field name

      if (collections.length == 0) {
        collections.push({
          collection_name,
          daily_volume: price,
          verified_creator_address: creator_address,
        });
      }

      const indexOf = collections.findIndex(
        (obj: any) => obj.collection_name === collection_name
      );

      if (indexOf != -1) {
        collections[indexOf].daily_volume += price;
      } else {
        collections.push({
          collection_name,
          daily_volume: price,
          verified_creator_address: creator_address,
        });
      }
    });
    consoleLog('collections.length before splice - ', collections.length);

    // highest vol to lowest, only return the top 5
    collections = collections
      .sort((b: any, a: any) => a?.daily_volume - b?.daily_volume)
      .splice(0, 5);

    return collections;
  } catch (error: any) {
    errorLog('getTopAptosCollectionsByDailyVolume - error: ', error?.messagge);
    return [];
  }
}

/**
 * Query all marketplaces to find latest sales event (topaz, wapal, bluemove, mercato)
 * @return {any} list of latest APT NFT sales events
 */
async function getLatestAptosVolume() {
  try {
    const dailyVolumeList: any = [];

    for (let i = 0; i < APTOS_SUPPORTED_MARKETPLACES.length; i++) {
      consoleLog(`On ${APTOS_SUPPORTED_MARKETPLACES[i].name}...`);

      let eventsList: any = [];
      let lastTransactionVersion = 0;

      for (
        let k = 0;
        k < (APTOS_SUPPORTED_MARKETPLACES[i].name == 'topaz' ? 10 : 1);
        k++
      ) {
        // Check if withdraw event has listing event for given marketplace
        const { errors, data } = await fetchLatestVolume(
          APTOS_SUPPORTED_MARKETPLACES[i].marketplaceContractAddress,
          lastTransactionVersion
        );
        if (errors) {
          consoleLog(
            `generateAllVolume - ${APTOS_SUPPORTED_MARKETPLACES[i].name} - errors: `,
            errors
          );
          return dailyVolumeList;
        }

        if (data?.events?.length > 0) {
          if (eventsList.length == 0) {
            eventsList = data?.events;
          } else {
            eventsList = [...data?.events, ...eventsList];
          }
        }

        if (data?.events?.length > 0) {
          if (data?.events[data.events.length - 1]?.transaction_version) {
            lastTransactionVersion =
              data.events[data.events.length - 1].transaction_version;
          }
        }
      }

      consoleLog(
        `generateAllVolume: ${APTOS_SUPPORTED_MARKETPLACES[i].name}, events length: `,
        eventsList.length
      );
      for (let k = 0; k < eventsList.length; k++) {
        if (eventsList[k]?.data?.[APTOS_SUPPORTED_MARKETPLACES[i].priceField]) {
          if (
            eventsList[k]?.type?.includes('BuyEvent') ||
            eventsList[k]?.type?.includes('ListingFilledEvent') ||
            eventsList[k]?.type?.includes('AcceptCollectionBidEvent') ||
            eventsList[k]?.type?.includes('FillCollectionBidEvent') ||
            eventsList[k]?.type?.includes('CollectionOfferFilledEvent') ||
            eventsList[k]?.type?.includes('BuyListingEvent') ||
            eventsList[k]?.type?.includes('SellEvent')
          )
            dailyVolumeList.push({
              price:
                eventsList[k]?.data?.[
                APTOS_SUPPORTED_MARKETPLACES[i].priceField
                ] / 100000000,
              marketplace: APTOS_SUPPORTED_MARKETPLACES[i].name,
              type: eventsList[k]?.type,
              ...(eventsList[k]?.data?.token_metadata && {
                creator_address:
                  eventsList[k]?.data?.token_metadata?.creator_address,
                collection_name:
                  eventsList[k]?.data?.token_metadata?.collection_name,
                name: eventsList[k]?.data?.token_metadata?.token_name,
              }),
              ...(eventsList[k]?.data?.token_id && {
                creator_address:
                  eventsList[k]?.data?.token_id?.token_data_id?.creator,
                collection_name:
                  eventsList[k]?.data?.token_id?.token_data_id?.collection,
                name: eventsList[k]?.data?.token_id?.token_data_id?.name,
              }),
            });
        } else {
          continue;
        }
      }
    }
    return dailyVolumeList;
  } catch (error) {
    consoleLog('getLatestAptosVolume - error - ', error);
    return [];
  }
}

// Takes a APT col's verifiedCreatorAddress and
// returns whether it exists in our db or not
const getAptosCollectionByVerifiedCreatorAddress = (
  verifiedCreatorAddress: string
): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const topCollectionsRef: any = createCollectionInstance(
        COLLECTION_NAMES.TOP_COLLECTIONS
      );

      const queryResult: any = await topCollectionsRef
        .where('verified_creator_address', '==', verifiedCreatorAddress)
        .limit(1)
        .get();

      if (queryResult?.size == 0) {
        resolve(null);
        return;
      } else {
        resolve(queryResult?.docs[0]);
        return;
      }
    } catch (error) {
      reject(error);
    }
  });
};

// Used to generate a new APT collection with correct fields
// and correct metadata
const generateAptosCollection = (
  verifiedCreatorAddress: string,
  collectionName: string
): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const now = new Date();
      const formattedTime = now.toISOString().slice(0, -1).replace('Z', '');

      let collection: any = {
        active: true,
        chain: 'aptos',
        created_at: timeStamp(),
        created_date: formattedTime,
        caught_up_txn_version: false,
        gallery: [],
        image_url: '',
        name: collectionName,
        lowercase_name: collectionName?.toLowerCase(),
        description: '',
        slug: collectionName?.split(' ')?.join('_')?.toLowerCase() + '_aptos',
        stats: {
          average_price: 0,
          floor_price: 0,
          market_cap: 0,
          num_owners: 0,
          one_day_average_price: 0,
          one_day_sales: 0,
          one_day_volume: 0,
          listed_count: 0,
          total_supply: 0,
          usd_floor_price: 0,
        },
        stats_eth: {
          floor_price: 0,
          market_cap: 0,
          one_day_volume: 0,
        },
        twitter_username: '',
        verified_creator_address: verifiedCreatorAddress,
      };

      // Adds gallery, image_url, description, and supply.
      collection = await getAptosColMetadata(collection);

      resolve(collection);
    } catch (error) {
      consoleLog('generateAptosCollection - error: ', error);
      reject(error);
    }
  });
};

// Returns a collection's description, supply, image_url, and gallery
// Adds logo to AWS automatically
const getAptosColMetadata = async (collection: any) => {
  try {
    const { data } = await fetchMyQueryCurrentTokenDatas(
      collection?.verified_creator_address
    );
    const { current_token_datas } = data;

    // consoleLog('current_token_datas - ', current_token_datas);

    if (current_token_datas.length > 0) {
      const fetchImageUrl = await getNftImageFromMetadataUri(
        current_token_datas[0].current_collection_data.metadata_uri.replace(
          'ipfs://',
          'https://ipfs.io/ipfs/'
        )
      );

      if (fetchImageUrl) {
        collection['image_url'] = fetchImageUrl?.replace(
          'ipfs://',
          'https://ipfs.io/ipfs/'
        );
      } else {
        collection['image_url'] =
          current_token_datas[0]?.current_collection_data?.metadata_uri?.replace(
            'ipfs://',
            'https://ipfs.io/ipfs/'
          );
      }

      // Add image url to AWS automatically
      if (collection['image_url'] && collection['image_url'] != '')
        await uploadImageToS3(collection['image_url']);

      collection['description'] =
        current_token_datas[0]?.current_collection_data?.description;
      collection['stats']['total_supply'] =
        current_token_datas[0]?.current_collection_data?.supply;

      const gallery = [];
      for (let i = 0; i < current_token_datas.length; i++) {
        const fetchGalleryImage = await getNftImageFromMetadataUri(
          current_token_datas[i]?.metadata_uri?.replace(
            'ipfs://',
            'https://ipfs.io/ipfs/'
          )
        );

        if (fetchGalleryImage) {
          gallery.push(
            fetchGalleryImage?.replace('ipfs://', 'https://ipfs.io/ipfs/')
          );
        } else {
          gallery.push(
            current_token_datas[i]?.metadata_uri?.replace(
              'ipfs://',
              'https://ipfs.io/ipfs/'
            )
          );
        }
      }
      collection['gallery'] = gallery;
    }

    return collection;
  } catch (error) {
    errorLog(
      `getAptosColMetadata - ${collection?.verified_creator_address} error:`,
      error
    );
    return null;
  }
};
