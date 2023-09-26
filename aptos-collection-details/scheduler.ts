import { createSchedulerInstance, consoleLog } from '../../service';
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
 *
 * Runs every 10 minutes
 */
export const updateAptosListingsActivityScheduler = createSchedulerInstance(
  '*/10 * * * *',
  { memory: '2GB', timeoutSeconds: 540 }
).onRun(async () => {
  consoleLog('Executing getAptosListingsActivityScheduler');
  const results = getAptosActiveListings();
  return results;
});

/**
 * Updates Aptos collections stats based on active listings and sales history.
 * Including: floor price, usd floor price, 1d volume, 1d sales, 1d avg price, listed count.
 *
 * Runs every 10 minutes
 */
export const updateAptosStatsScheduler = createSchedulerInstance(
  '*/10 * * * *',
  { memory: '2GB', timeoutSeconds: 540 }
).onRun(async () => {
  consoleLog('Executing getAptosFloorPriceScheduler');
  const results = updateAptosCollectionStats();
  return results;
});

/**
 * Updates Aptos collections number of owners (num_owners) for each collection
 *
 * Runs every 2 hours
 */
export const updateAptosNumOwnersScheduler = createSchedulerInstance(
  '0 */2 * * *',
  { memory: '2GB', timeoutSeconds: 540 }
).onRun(async () => {
  consoleLog('Executing updateAptosNumOwnersScheduler');
  const results = updateAptosCollectionNumOwners();
  return results;
});

/**
 * Fetches and add new Aptos collections not currently supported
 * 1) Checks daily volume across 4 marketplaces and finds the top 5 collections by daily volume
 * 2) Checks each collection by verified_creator_address to see if its already added in top_collections
 * 3) If it's not, adds the collection and set the field caught_up_txn_version=false
 *
 * Runs once per day
 */
export const fetchNewAptosCollectionsScheduler = createSchedulerInstance(
  '10 1 */1 * *',
  { memory: '2GB', timeoutSeconds: 540 }
).onRun(async () => {
  consoleLog('Executing fetchNewAptosCollectionsScheduler');
  const results = fetchNewAptosCollectionsAutomated();
  return results;
});

/**
 * Catches up onchain listings/activity for newly added Aptos collections
 * 1) Finds oldest collection by created_at where caught_up_txn=false
 * 2) Fetch max of 200 txns for the collection. When <200 txns returned, set caught_up_txn=true
 * 3) Parse txns and add active listings and salees history
 *
 * Runs every 10 minutes
 */
export const catchUpAptosActivityScheduler = createSchedulerInstance(
  '*/10 * * * *',
  { memory: '2GB', timeoutSeconds: 540 }
).onRun(async () => {
  consoleLog('Executing catchUpAptosActivityScheduler');
  const results = catchUpAptosCollections();
  return results;
});
