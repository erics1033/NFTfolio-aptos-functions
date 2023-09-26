// eslint-disable-next-line @typescript-eslint/no-var-requires
const fetch = require('node-fetch');

/**
 * Used to fetch latest token events based on a creator address, that happen before a transaction version
 * @param {string} creator_address aptos collection creator addr
 * @param {number} transaction_version get events after txn version
 * @param {boolean} catchUpMode if catch up mode, increases limit
 * @return {any} list of token events by creator address
 */
export function fetchTokenEvents(
  creator_address: string,
  transaction_version: number,
  catchUpMode: boolean
) {
  const operationGetRecentListings = `
query MyQuery {
  token_activities_aggregate(
    order_by: {transaction_timestamp: asc}
    where: {creator_address: {_eq: "${creator_address}"}, _and: {transaction_version: {_gt: "${transaction_version}"}}}
    limit: ${catchUpMode ? 100 : 65}
  ) {
    nodes {
      creator_address
      current_token_data {
        collection_data_id_hash
      }
      from_address
      to_address
      token_data_id_hash
      transaction_version
      transfer_type
      transaction_timestamp
      name
    }
  }
}
`;
  return fetchGraphQL(operationGetRecentListings, 'MyQuery', {});
}

/**
 * Used to fetch the listing price of a withdraw event for an NFT item
 * @param {string} fromAddress marketplace or sender
 * @param {string} transactionVersion txn of listing event
 * @return {any} listing event
 */
export function fetchListingPriceQuery(
  fromAddress: string,
  transactionVersion: string
) {
  const operationsDoc = `
  query MyQuery {
    events(
      where: {account_address: {_eq: "${fromAddress}"}, transaction_version: {_eq: "${transactionVersion}"}}
    ) {
      data
      type
    }
  }
`;
  return fetchGraphQL(operationsDoc, 'MyQuery', {});
}

/**
 * Used to fetch the metadata_uri for an NFT item
 * @param {string} tokenDataIdHash unique NFT token data ID hash
 * @return {any} NFT data with metadata_uri
 */
export function fetchMetadataUri(tokenDataIdHash: string) {
  const operationsDoc = `
  query MyQuery {
    token_datas(
      where: {token_data_id_hash: {_eq: "${tokenDataIdHash}"}}
    ) {
      metadata_uri
    }
  }
  
`;
  return fetchGraphQL(operationsDoc, 'MyQuery', {});
}

/**
 * Used to fetch image_url, supply, description and NFT gallery for new Aptos collections
 * @param {string} verifiedCreatorAddress APT col verified addrs
 * @return {any} NFT item and NFT collection data
 */
export function fetchMyQueryCurrentTokenDatas(verifiedCreatorAddress: string) {
  const operationGetRecentListings = `
  query MyQuery {
    current_token_datas(
      where: {creator_address: {_eq: "${verifiedCreatorAddress}"}}
      limit: 9
    ) {
      current_collection_data {
        collection_data_id_hash
        collection_name
        creator_address
        description
        description_mutable
        last_transaction_timestamp
        last_transaction_version
        maximum
        supply
        metadata_uri
      }
      metadata_uri
      token_data_id_hash
    }
  }
`;

  return fetchGraphQL(operationGetRecentListings, 'MyQuery', {});
}

/**
 * Used to fetch latest volume from any given NFT marketplace address
 * @param {string} marketplaceAddress NFT marketplace address
 * @param {number} lastTransactionVersion last txn version fetched already
 * @return {any} events data (including sales)
 */
export function fetchLatestVolume(
  marketplaceAddress: string,
  lastTransactionVersion: number
) {
  const operationsDoc =
    lastTransactionVersion == 0
      ? `
  query MyQuery {
    events(
      where: {account_address: {_eq: "${marketplaceAddress}"}}
      order_by: {transaction_version: desc}
      limit: 100
    ) {
      event_index
      data
      account_address
      type
      transaction_version
    }
  }
`
      : `
query MyQuery {
  events(
    where: {account_address: {_eq: "${marketplaceAddress}"}, _and: {transaction_version: {_lt: "${lastTransactionVersion}"}}}
    order_by: {transaction_version: desc}
    limit: 100
  ) {
    event_index
    data
    account_address
    type
    transaction_version
  }
}
`;
  return fetchGraphQL(operationsDoc, 'MyQuery', {});
}

/**
 * Used to fetch an Aptos collection's unique num of owners
 * @param {string} verifiedCreatorAddress APT col verified addrs
 * @return {any} num owners count
 */
export function fetchAptosUniqueOwners(verifiedCreatorAddress: string) {
  const operationGetRecentListings = `
  query MyQuery {
    current_collection_ownership_v2_view_aggregate(
      where: {creator_address: {_eq: "${verifiedCreatorAddress}"}}
    ) {
      aggregate {
        count(distinct: true)
      }
    }
  }
`;
  return fetchGraphQL(operationGetRecentListings, 'MyQuery', {});
}

/**
 * GraphQL call to fetch on-chain Aptos data
 * @param {any} operationsDoc query name
 * @param {any} operationName operation name
 * @param {any} variables query variables
 * @return {any} results from fetch
 */
async function fetchGraphQL(
  operationsDoc: any,
  operationName: any,
  variables: any
) {
  const result = await fetch(
    'https://indexer.mainnet.aptoslabs.com/v1/graphql',
    {
      method: 'POST',
      body: JSON.stringify({
        query: operationsDoc,
        variables: variables,
        operationName: operationName,
      }),
    }
  );

  return await result.json();
}
