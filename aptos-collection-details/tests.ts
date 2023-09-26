import { createFunctionInstance, errorLog } from '../../service';
import { getAptosActiveListings, updateAptosCollectionStats } from './services';
import {
  catchUpAptosCollections,
  fetchNewAptosCollectionsAutomated,
} from './services/aptos-automated.service';
import { updateAptosCollectionNumOwners } from './services/aptos-owners.service';

/*
 * Used to fetch on-chain Aptos events (listings, delistings, sales, price changes)
 * for adding active listings and latest sales activity.
 * Only runs for Aptos collections where caught_up_txn=true
 */
export const updateAptosListingsFunction = createFunctionInstance().onRequest(
  async (request, response): Promise<any> => {
    try {
      const result = await getAptosActiveListings();
      return response.send({ result });
    } catch (error) {
      errorLog('updateAptosListingsFunction Error', error);
    }
  }
);

/**
 * Updates Aptos collections stats based on active listings and sales history.
 * Including: floor price, usd floor price, 1d volume, 1d sales, 1d avg price, listed count.
 */
export const updateAptosStatsFunction = createFunctionInstance().onRequest(
  async (request, response): Promise<any> => {
    try {
      const result = await updateAptosCollectionStats();
      return response.send({ result });
    } catch (error) {
      errorLog('updateAptosStatsFunction Error', error);
    }
  }
);

/**
 * Updates Aptos collections number of owners (num_owners) for each collection
 */
export const updateAptosNumOwnersFunction = createFunctionInstance().onRequest(
  async (request, response): Promise<any> => {
    try {
      const result = await updateAptosCollectionNumOwners();
      return response.send({ result });
    } catch (error) {
      errorLog('updateAptosNumOwnersFunction Error', error);
    }
  }
);

/**
 * Fetches and add new Aptos collections not currently supported
 */
export const fetchNewAptosColsAutomatedFunction =
  createFunctionInstance().onRequest(
    async (request, response): Promise<any> => {
      try {
        const result = await fetchNewAptosCollectionsAutomated();
        return response.send({ result });
      } catch (error) {
        errorLog('fetchNewAptosColsAutomatedFunction Error', error);
      }
    }
  );

/**
 * Catches up onchain listings/activity for newly added Aptos collections
 * 1) Finds oldest collection by created_at where caught_up_txn=false
 * 2) Fetch max of 200 txns for the collection. When <200 txns returned, set caught_up_txn=true
 * 3) Parse txns and add active listings and salees history
 */
export const catchUpAptosColsFunction = createFunctionInstance().onRequest(
  async (request, response): Promise<any> => {
    try {
      const result = await catchUpAptosCollections();
      return response.send({ result });
    } catch (error) {
      errorLog('catchUpAptosColsFunction Error', error);
    }
  }
);

