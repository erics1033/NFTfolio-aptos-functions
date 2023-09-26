import * as admin from 'firebase-admin';
import { ISchedulerConfigDocument } from '../../../models';
import {
  consoleLog,
  convertTimeStamp,
  createCollectionInstance,
  createFirestoreBatchInstance,
  errorLog,
  executeWriteBatch,
  getSchedulerConfig,
  timeStamp,
  updateOrDeleteSchedulerConfig,
} from '../../../service';
import {
  APTOS_SUPPORTED_MARKETPLACES,
  COLLECTION_NAMES,
} from '../../../values';
import { fetchListingPriceQuery, fetchTokenEvents } from './graphql.service';
import {
  getAptosNftImage,
  removeDuplicatesWithHighestVersion,
} from './helper.service';


// Uses scheduler config to keep track of last aptos top_collections
// that it fetched active listings for.
//
// Gets 65 latest events per collection and adds active listings / sales history
export const getAptosActiveListings = async () => {
  try {
    const limit = 10;
    const maxIteration = 2;

    let startAfter: any = null; // offset data for retrieving top_collection
    const batchPayload: any = [];
    const batchResponse: any = [];

    const configData = await getSchedulerConfig(
      'UPDATE_APTOS_COLLECTIONS',
      true
    );
    if (configData) {
      startAfter = configData?.startAfter;
    }

    for (let i = 0; i < maxIteration; i++) {
      consoleLog(`-------- Batch - ${i} | startAfter - ${startAfter}`);
      const payload = {
        startAfter,
        limit,
      };
      batchPayload.push(payload);
      const response = await updateGroupedAptosListings(payload);
      batchResponse.push(response);

      startAfter = response?.startAfter;
    }

    Promise.all([batchResponse]);

    const config: ISchedulerConfigDocument = {
      ...(configData?.id && { id: configData?.id }), // Append id if exists
      schedulerName: 'UPDATE_APTOS_COLLECTIONS',
      startAfter,
    };

    // Only update scheduler config
    await updateOrDeleteSchedulerConfig(config, true);

    return { success: true };
  } catch (error) {
    errorLog('Function | getAptosActiveListings | Error ', error);
    return { success: false, error };
  }
};

// Loops through top_collections,
// calls function to get a collection's active listings, stale listings (since last checked)
// Updates/deletes listings accordingly, saves collection's last fetched time
export const executeAptosListingsBatchApiRequest = (
  queryData: any,
  batchCollection: any,
  catchUpMode: boolean
): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve) => {
    try {
      if (queryData.length == 0) resolve([]);
      const collectionArray: any = [];

      for (let i = 0; i < queryData.length; i++) {
        // Skip over new collections not caught up, when on main scheduler
        if (!catchUpMode && queryData[i]?.caught_up_txn_version == false)
          continue;

        const verifiedCreatorAddress = queryData[i]?.verified_creator_address;
        const name = queryData[i]?.name;
        const savedTransactionVersion = queryData[i]?.last_transaction_version;

        consoleLog(`Processing ${name}...`);
        if (
          // eslint-disable-next-line no-prototype-builtins
          queryData.hasOwnProperty(i) &&
          // eslint-disable-next-line no-prototype-builtins
          queryData[i].hasOwnProperty('verified_creator_address') &&
          verifiedCreatorAddress &&
          verifiedCreatorAddress != ''
        ) {
          await getAptosCollectionListings(
            verifiedCreatorAddress,
            savedTransactionVersion,
            catchUpMode
          )
            .then(async (result: any) => {
              if (result && result?.status == 200) {
                const {
                  activeListings,
                  depositActivity,
                  lastTransactionVersion,
                  newCaughtUp,
                } = result;

                const aptosListingsRef: any = createCollectionInstance(
                  COLLECTION_NAMES.APTOS_LISTINGS
                );
                const aptosActivitiesRef: any = createCollectionInstance(
                  COLLECTION_NAMES.APTOS_ACTIVITIES
                );

                consoleLog(
                  `${name} deposit activity amount:`,
                  depositActivity.length
                );

                // Step 1. Delete stale listings
                // Loop through stale listings, if one exists, delete it

                // Step 2. Add/update active listings
                // If active listing document already exists, merge. Otherwise add

                for (let j = 0; j < depositActivity.length; j++) {
                  consoleLog(
                    `#${j}. ${depositActivity[j].name}, type: ${depositActivity[j].type}...`
                  );

                  // Remove any stale listings from the listings list
                  // Delete any stale listings with a deposit event (could be sale or delisting)
                  const staleListingDoc = await getAptosListingItem(
                    depositActivity[j].token_data_id_hash
                  );

                  if (staleListingDoc != null && staleListingDoc?.id) {
                    consoleLog(
                      `Deleting stale listing, as salesActivity[${j}] - NFT name: `,
                      depositActivity[j].name
                    );
                    await aptosListingsRef.doc(staleListingDoc.id).delete();
                  }

                  if (
                    (depositActivity[j]?.type?.includes('BuyEvent') ||
                      depositActivity[j]?.type?.includes(
                        'ListingFilledEvent'
                      ) ||
                      depositActivity[j]?.type?.includes(
                        'AcceptCollectionBidEvent'
                      ) ||
                      depositActivity[j]?.type?.includes(
                        'FillCollectionBidEvent'
                      ) ||
                      depositActivity[j]?.type?.includes(
                        'CollectionOfferFilledEvent'
                      ) ||
                      depositActivity[j]?.type?.includes('BuyListingEvent') ||
                      depositActivity[j]?.type?.includes('SellEvent')) &&
                    depositActivity[j]?.price != null
                  ) {
                    // Check if sales activity exists by transaction_version AND token_data_id_hash
                    const existingActivityDoc = await getAptosActivityItem(
                      depositActivity[j].transaction_version,
                      depositActivity[j].token_data_id_hash
                    );

                    // Check if sales price already is in collection, if not, add it
                    if (!existingActivityDoc) {
                      consoleLog(
                        `Adding new sales activity [${j}] doc, name: ${depositActivity[j].name}`
                      );
                      const activityData = {
                        top_collection_id: queryData[i]?.id,
                        collection_name: queryData[i]?.name,
                        slug: queryData[i]?.slug,
                        verified_creator_address:
                          queryData[i]?.verified_creator_address,
                        ...depositActivity[j],
                        type: depositActivity[j].type.split('::').pop(),
                      };

                      const imageUrl = await getAptosNftImage(
                        depositActivity[j].token_data_id_hash
                      );
                      if (imageUrl) {
                        activityData['image_url'] = imageUrl;
                      }

                      await aptosActivitiesRef.add(activityData);
                    } else {
                      consoleLog(
                        `Sales activity [${j}] already exists... - ${depositActivity[j].name}`
                      );
                    }
                  } else {
                    consoleLog(
                      `Deposit activity [${j}] doesnt have correct type: ${depositActivity[j]?.type}, or price: ${depositActivity[j]?.price}`
                    );
                  }
                }

                consoleLog(
                  `${name} active listings amount:`,
                  activeListings.length
                );

                // Update new listing if already exists, otherwise, add the new listing
                for (let j = 0; j < activeListings.length; j++) {
                  const listingData = {
                    created_at: timeStamp(),
                    top_collection_id: queryData[i]?.id,
                    collection_name: queryData[i]?.name,
                    slug: queryData[i]?.slug,
                    verified_creator_address:
                      queryData[i]?.verified_creator_address,
                    ...activeListings[j],
                    type: activeListings[j].type.split('::').pop(),
                  };

                  const activeListingDoc = await getAptosListingItem(
                    activeListings[j].token_data_id_hash
                  );
                  if (activeListingDoc != null && activeListingDoc?.id) {
                    consoleLog(`Updating active listing [${j}] doc...`);
                    aptosListingsRef
                      .doc(activeListingDoc.id)
                      .set(listingData, { merge: true });
                  } else {
                    consoleLog(`Adding new active listing [${j}] doc...`);

                    const imageUrl = await getAptosNftImage(
                      activeListings[j].token_data_id_hash
                    );
                    if (imageUrl) {
                      listingData['image_url'] = imageUrl;
                    }

                    await aptosListingsRef.add(listingData);
                  }
                }

                // Update aptos top collection's last transaction version and last_updated_listings timestamp
                const ref = createCollectionInstance(
                  COLLECTION_NAMES.TOP_COLLECTIONS
                ).doc(queryData[i]?.id);

                consoleLog(
                  'batchCollection lastTransactionVersion: ',
                  lastTransactionVersion
                );
                consoleLog('batchCollection catchUpMode: ', catchUpMode);
                consoleLog('batchCollection set - newCaughtUp: ', newCaughtUp);

                batchCollection.set(
                  ref,
                  {
                    ...(lastTransactionVersion != null && {
                      last_transaction_version: lastTransactionVersion,
                    }),
                    ...(newCaughtUp == true &&
                      catchUpMode == true && {
                      caught_up_txn_version: true,
                    }),
                    last_updated_listings_at: timeStamp(),
                  },
                  { merge: true }
                );
              }
            })
            .catch((error) => {
              errorLog('executeAptosCollectionsBatchApiRequest error', error);
            });
        }
        collectionArray.push(queryData[i]);
      }

      resolve(collectionArray);
    } catch (error) {
      errorLog(
        'Error - executeAptosCollectionsBatchApiRequest Parsing Data',
        error
      );
    }
  });
};

/** ****************************************
 *********** PRIVATE FUNCTIONS *************
 ******************************************/

// Starts after last fetched aptos collection,
// Calls function to update active listings, pass back new startAfter value
const updateGroupedAptosListings = (payload: any): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const updateBatch = createFirestoreBatchInstance();

      const { startAfter, limit } = payload;
      let newStartAfter = startAfter;

      /** Top Collection Firestore Reference **/
      let topCollectionRef: any = createCollectionInstance(
        COLLECTION_NAMES.TOP_COLLECTIONS
      );

      /** Get top collection ordered by slug **/
      topCollectionRef = await topCollectionRef
        .where('active', '==', true)
        .where('chain', '==', 'aptos')
        .orderBy(admin.firestore.FieldPath.documentId(), 'desc');

      const queryData: any = [];

      if (startAfter) {
        topCollectionRef = topCollectionRef.startAfter(startAfter);
      }

      const queryResult: any = await topCollectionRef.limit(limit).get();

      consoleLog('To update collection size', queryResult?.size);

      // Get document data from query result
      await queryResult.forEach(async (doc: any) => {
        const id = doc.id;
        const data = doc.data();

        queryData.push({ ...data, id });
      });

      // Update Aptos active listings
      const parsedCollection = await executeAptosListingsBatchApiRequest(
        queryData,
        updateBatch,
        false
      );

      // Execute update aptos collections batch
      await executeWriteBatch(
        updateBatch,
        'Update aptos collections data batch'
      );

      console.log('parsedCollection.length - ', parsedCollection.length);

      newStartAfter =
        parsedCollection.length > 0
          ? parsedCollection[parsedCollection.length - 1].id
          : null;

      console.log(
        `${parsedCollection[parsedCollection.length - 1].name
        }, newStartAfter: ${newStartAfter}`
      );

      resolve({
        startAfter: newStartAfter,
        limit,
        collectionUpdated: parsedCollection.length,
      });
    } catch (error) {
      errorLog('updateGroupedAptosCollections Error', error);
      reject(error);
    }
  });
};

/**
 * Used to fetch active listing of an Aptos NFT collection based on the creator address
 * @param {string} verifiedCreatorAddress aptos collection's verified creator address
 * @param {number | null} savedTransactionVersion if the user has a saved txn from a previous run
 * @param {boolean} catchUpMode settings for when catching up a listing
 * @return {any} all aptos active listings, stale listings, lastTransactionVersion, status
 */
async function getAptosCollectionListings(
  verifiedCreatorAddress: string,
  savedTransactionVersion: number | null,
  catchUpMode: boolean
) {
  try {
    if (!verifiedCreatorAddress || verifiedCreatorAddress == '')
      return {
        activeListings: [],
        depositActivity: [],
        lastTransactionVersion: null,
        status: 400,
        newCaughtUp: false,
      };

    let onchainActivity: any = [];

    let lastTransactionVersion = savedTransactionVersion || 250000000;

    consoleLog(
      `verifiedCreatorAddress: ${verifiedCreatorAddress}, OLD lastTransactionVersion: ${lastTransactionVersion}`
    );

    // Loops through to get the latest withdraw events
    // 200 events if catchupmode, otherwise 65 events
    for (let i = 0; i < (catchUpMode ? 2 : 1); i++) {
      const { errors, data } = await fetchTokenEvents(
        verifiedCreatorAddress,
        lastTransactionVersion,
        catchUpMode
      );

      if (errors) {
        consoleLog(
          `Errors fetchTokenEvents, verifiedCreatorAddress: ${verifiedCreatorAddress}, lastTransactionVersion: ${lastTransactionVersion}`
        );
        console.error(errors);
        return {
          activeListings: [],
          depositActivity: [],
          lastTransactionVersion: null,
          status: 400,
          newCaughtUp: false,
        };
      }

      if (onchainActivity.length === 0) {
        onchainActivity = data.token_activities_aggregate.nodes;
      } else {
        onchainActivity = [
          ...data.token_activities_aggregate.nodes,
          ...onchainActivity,
        ];
      }

      // Fix potential null lastTransactionVersion error here.
      if (data?.token_activities_aggregate?.nodes?.length > 0) {
        for (
          let n = data?.token_activities_aggregate?.nodes.length - 1;
          n >= 0;
          n--
        ) {
          if (data?.token_activities_aggregate?.nodes[n]?.transaction_version) {
            lastTransactionVersion =
              data?.token_activities_aggregate?.nodes[n]?.transaction_version;
            break;
          }
        }
      }
      if (catchUpMode && data.token_activities_aggregate.nodes.length < 100) {
        consoleLog(
          `Catch up mode, Under 100 events: ${data.token_activities_aggregate.nodes.length} - breaking out of loop...`
        );
        break; // break out of for loop if under 100 limit
      }
    }
    consoleLog(`onchainActivity.length: ${onchainActivity.length}`);

    consoleLog(
      `verifiedCreatorAddress: ${verifiedCreatorAddress}, - NEW lastTransactionVersion: ${lastTransactionVersion}`
    );

    const recentListingList: any = [];
    const depositActivity: any = [];
    const recentSalesList: any = [];

    // Only takes the latest listing/delisting/sale event per NFT item (based on name)
    const latestListDelistActivity =
      removeDuplicatesWithHighestVersion(onchainActivity);

    // FOR LISTINGS ACTIVITY
    consoleLog(
      'latestListDelistActivity length - ',
      latestListDelistActivity.length
    );

    // Loops through latest activity and only takes listing events
    for (let i = 0; i < latestListDelistActivity.length; i++) {
      if (
        latestListDelistActivity[i].transfer_type == '0x3::token::WithdrawEvent'
      ) {
        recentListingList.push({
          name: latestListDelistActivity[i].name,
          from_address: latestListDelistActivity[i].from_address,
          transaction_timestamp:
            latestListDelistActivity[i].transaction_timestamp,
          transaction_version: latestListDelistActivity[i].transaction_version,
          token_data_id_hash: latestListDelistActivity[i].token_data_id_hash,
        });
      }
    }
    consoleLog('recentListingList length - ', recentListingList.length);

    // FOR SALES ACTIVITY
    consoleLog('onchainActivity length - ', onchainActivity.length);

    for (let i = 0; i < onchainActivity.length; i++) {
      if (onchainActivity[i].transfer_type != '0x3::token::WithdrawEvent') {
        recentSalesList.push({
          ...onchainActivity[i],
          name: onchainActivity[i].name,
          from_address: onchainActivity[i].from_address,
          transaction_version: onchainActivity[i].transaction_version,
          token_data_id_hash: onchainActivity[i].token_data_id_hash,
        });
      }
    }
    consoleLog('recentSalesList.length - ', recentSalesList.length);

    // Use all onchain activity including duplicates for sales
    for (let i = 0; i < recentSalesList.length; i++) {
      // Get the sales price and ensure it's a sale and not a delisting
      const {
        price: salePrice,
        marketplace,
        type,
      } = await fetchListingItemPrice(
        recentSalesList[i].transaction_version,
        recentSalesList[i].name
      );

      depositActivity.push({
        price: salePrice,
        marketplace,
        type,
        name: recentSalesList[i].name,
        to_address: recentSalesList[i].to_address,
        block_datetime: convertTimeStamp(
          recentSalesList[i].transaction_timestamp
        ),
        transaction_version: recentSalesList[i].transaction_version,
        token_data_id_hash: recentSalesList[i].token_data_id_hash,
      });
    }

    consoleLog('depositActivity.length - ', depositActivity.length);

    const activeListings: any = [];
    consoleLog('recentListingList length - ', recentListingList.length);

    for (let j = 0; j < recentListingList.length; j++) {
      const {
        price: listingPrice,
        marketplace,
        type,
      } = await fetchListingItemPrice(
        recentListingList[j].transaction_version,
        recentListingList[j].name
      );

      // Check if type is a listing before adding in case it's a wapal or mercato instant sale
      if (
        !type?.includes('CollectionOfferFilledEvent') &&
        !type?.includes('FillCollectionBidEvent') &&
        !type?.includes('BuyEvent') &&
        !type?.includes('BuyListingEvent') &&
        !type?.includes('SellEvent') &&
        !type?.includes('ListingFilledEvent') &&
        !type?.includes('AcceptCollectionBidEvent')
      ) {
        if (!isNaN(Number(listingPrice)) && listingPrice) {
          activeListings.push({
            price: listingPrice,
            marketplace,
            type,
            ...recentListingList[j],
          });
        }
      }
    }

    consoleLog('activeListings.length -', activeListings.length);
    consoleLog('onchainActivity.length -', onchainActivity.length);
    if (catchUpMode) {
      consoleLog('newCaughtUp -', onchainActivity.length < 200);
    }

    return {
      activeListings,
      depositActivity,
      lastTransactionVersion,
      status: 200,
      newCaughtUp: onchainActivity.length < 200, // if less than 200 req, collection is all caught up.
    };
  } catch (error) {
    errorLog('getAptosCollectionListings - error: ', error);
    return {
      activeListings: [],
      depositActivity: [],
      lastTransactionVersion: null,
      status: 500,
      newCaughtUp: false,
    };
  }
}

/**
 * Fetch listing price of a nft
 * Queries all marketplaces to find nft listing price: topaz, wapal, bluemove, mercato, etc.
 *
 * @param {string} transactionVersion txnVersion of listing
 * @param {string} name NFT name
 * @return {any} listing price, marketplace, event type
 */
async function fetchListingItemPrice(transactionVersion: string, name: string) {
  try {
    // Check if withdraw event has listing event for given marketplace
    for (let i = 0; i < APTOS_SUPPORTED_MARKETPLACES.length; i++) {
      const { errors, data } = await fetchListingPriceQuery(
        APTOS_SUPPORTED_MARKETPLACES[i].marketplaceContractAddress,
        transactionVersion
      );

      if (errors) {
        console.error(errors);
        consoleLog(`fetchListingItemPrice - ${name} - errors: `, errors);
        return { price: null, marketplace: '', type: '' };
      }

      // If it's a delist event, will try and return any list events after before returning delist event
      let tempData: any = null;

      // Check if marketplace listing price exists
      for (let k = 0; k < data?.events.length; k++) {
        if (
          data?.events[k]?.data?.[APTOS_SUPPORTED_MARKETPLACES[i].priceField] &&
          (data?.events[k]?.type?.includes('DelistEvent') ||
            data?.events[k]?.type?.includes('ListingCanceledEvent'))
        ) {
          consoleLog(
            `${name} - Found DelistEvent or ListingCanceledEvent, setting tempData...`
          );
          tempData = {
            price:
              data?.events[k]?.data?.[
              APTOS_SUPPORTED_MARKETPLACES[i].priceField
              ] / 100000000,
            marketplace: APTOS_SUPPORTED_MARKETPLACES[i].name,
            type: data?.events[k]?.type,
          };
        }
        // Return price if not a delist event
        else if (
          data?.events[k]?.data?.[APTOS_SUPPORTED_MARKETPLACES[i].priceField]
        ) {
          return {
            price:
              data?.events[k]?.data?.[
              APTOS_SUPPORTED_MARKETPLACES[i].priceField
              ] / 100000000,
            marketplace: APTOS_SUPPORTED_MARKETPLACES[i].name,
            type: data?.events[k]?.type,
          };
        }
      }

      // Return delist data if it exists and no relist info after it
      if (tempData != null) {
        consoleLog(`${name} - Returning delist data... `, tempData);
        return tempData;
      }
    }

    // If no listings found for supported marketplaces, return null
    consoleLog(`No listings found for ${name}`);
    return { price: null, marketplace: '', type: '' };
  } catch (error) {
    errorLog(`fetchListingItemPrice - ${name} error - `, error);
    return { price: null, marketplace: '', type: '' };
  }
}

// Check if Aptos listing item exists in database by tokenDataIdHash
const getAptosListingItem = (tokenDataIdHash: string): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      /** Top Collection Firestore Reference **/
      const aptosListingsRef: any = createCollectionInstance(
        COLLECTION_NAMES.APTOS_LISTINGS
      );

      const queryResult: any = await aptosListingsRef
        .where('token_data_id_hash', '==', tokenDataIdHash)
        .limit(1)
        .get();

      if (queryResult?.size == 0) {
        resolve(null);
        return;
      } else {
        // Get document data from query result
        resolve(queryResult?.docs[0]);
        return;
      }
    } catch (error) {
      errorLog('getAptosListingItem - error: ', error);
      reject(error);
    }
  });
};

// Checks if aptos sales activity item exists in database by tokenDataIdHash
const getAptosActivityItem = (
  transactionVersion: string,
  tokenDataIdHash: string
): Promise<any> => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const aptosActivitiesRef: any = createCollectionInstance(
        COLLECTION_NAMES.APTOS_ACTIVITIES
      );

      const queryResult: any = await aptosActivitiesRef
        .where('transaction_version', '==', transactionVersion)
        .where('token_data_id_hash', '==', tokenDataIdHash)
        .limit(1)
        .get();

      if (queryResult?.size == 0) {
        resolve(null);
        return;
      } else {
        // Get document data from query result
        resolve(queryResult?.docs[0]);
        return;
      }
    } catch (error) {
      errorLog('getAptosActivityItem - error: ', error);
      reject(error);
    }
  });
};