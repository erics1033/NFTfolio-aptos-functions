import axios from 'axios';
import { fetchMetadataUri } from './graphql.service';
import { consoleLog } from '../../../service';

/**
 * 1) Takes in all the events activity for an APT collection
 * 2) Filters through events to only save the latest withdraw/deposit events
 * for each unique NFT item (listings, delistings and sales activity)
 * This overrides old listings / sales so scheduler only adds active listings.
 *
 * 3) Converts the map back to an array and returns filtered non-duplicate token activity list
 *
 * @param {any} arr array of token activity passed
 * @return {any} filtered non-duplicate token activity list
 */
export function removeDuplicatesWithHighestVersion(arr: any) {
  const uniqueElementsMap = new Map();

  // Loop through the array and keep only elements with the highest transaction_version
  arr.forEach((element: any) => {
    const { token_data_id_hash, transaction_version, transfer_type } = element;
    // Filters to only be lisitings, delistings, sales
    if (
      transfer_type === '0x3::token::DepositEvent' ||
      transfer_type === '0x3::token::WithdrawEvent'
    ) {
      if (
        !uniqueElementsMap.has(token_data_id_hash) ||
        uniqueElementsMap.get(token_data_id_hash).transaction_version <=
        transaction_version
      ) {
        uniqueElementsMap.set(token_data_id_hash, element);
      }
    }
  });

  // Convert the map values back to an array
  const result = Array.from(uniqueElementsMap.values());

  return result;
}

/**
 * Used to get an Aptos NFT item's onchain image by token id hash
 * @param {string} tokenDataIdHash - NFT's unique token data id hash
 * @return {any} nftImage string|null
 */
export async function getAptosNftImage(tokenDataIdHash: string) {
  try {
    const { data: metadataData, errors } = await fetchMetadataUri(
      tokenDataIdHash
    );

    if (errors) {
      consoleLog(`getAptosNftImage ${tokenDataIdHash} - errors: `, errors);
      return null;
    } else {
      let metadataUriPath = metadataData?.token_datas[0]?.metadata_uri;
      if (metadataUriPath.includes('ipfs://')) {
        metadataUriPath = metadataUriPath.replace(
          'ipfs://',
          'https://ipfs.io/ipfs/'
        );
      }

      const { status, data }: any = await axios
        .get(metadataUriPath, {})
        .catch(() => {
          consoleLog('axios error - fetching: ', metadataUriPath);
          return null;
        });
      if (status === 200 && data?.image) {
        return data?.image?.replace('ipfs://', 'https://ipfs.io/ipfs/');
      } else {
        return null;
      }
    }
  } catch (error) {
    consoleLog(`getAptosNftImage ${tokenDataIdHash} - error: `, error);
    return null;
  }
}

/**
 * Used to get an Aptos NFT's onchain image if have the metadata URI path
 * @param {string} metadataUriPath - metadata uri
 * @return {any} nftImage string|null
 */
export async function getNftImageFromMetadataUri(metadataUriPath: string) {
  try {
    const { status, data }: any = await axios
      .get(metadataUriPath, {})
      .catch((error: any) => {
        consoleLog('axios error ', error);
        return null;
      });
    if (status === 200 && data?.image) {
      return data?.image?.replace('ipfs://', 'https://ipfs.io/ipfs/');
    } else {
      return null;
    }
  } catch (error) {
    consoleLog(
      `getNftImageFromMetadataUri ${metadataUriPath} - error: `,
      error
    );
    return null;
  }
}



/**
 * Used to get an Aptos NFT collection's via creator address
 * @param {string} creatorAddress - verified creator address
 * @return {any} slug string|null
 */
export async function getAptosCollectionSlug(creatorAddress: string | null) {
  try {
    if (!creatorAddress) return null;

    // Note: replace this with your own DB to fetch the Aptos NFT collection 
    const queryResult = await createCollectionInstance(
      COLLECTION_NAMES.TOP_COLLECTIONS
    )
      .where('verified_creator_address', '==', creatorAddress)
      .limit(1)
      .get();

    if (queryResult?.size == 0) {
      return null;
    } else {
      return queryResult?.docs[0]?.data()?.slug;
    }
  } catch (error) {
    errorLog(`getAptosCollectionSlug ${creatorAddress} - error: `, error);
    return null;
  }
}
