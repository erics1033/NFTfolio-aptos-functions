import { consoleLog, errorLog, getCollection } from '../../../../service';
import { COLLECTION_NAMES } from '../../../../values';
import { getAptWalletNfts } from './graphql.service';
import {
  getAptosCollectionSlug,
  getNftImageFromMetadataUri,
} from './helper.service';
import { WalletCollection, WalletGallery } from '../../models';
import { cloneDeep, findIndex } from 'lodash';

export const requestAptosWalletCollection = async (
  wallet: string
): Promise<{
  collectionList: WalletCollection[],
  galleryList: WalletGallery[],
}> => {
  try {
    const limit = 50; // how many NFTs are retrieved on each call
    let result: any = [];
    let hasCollection = true;
    let offset = 0;
    let retries = 3;

    consoleLog('Calling requestAptosWalletCollection, wallet: ', wallet);

    do {
      try {
        const response: any = await getAptWalletNfts(wallet, offset);

        if (response) {
          const { current_token_ownerships_v2 } = response?.data;

          consoleLog(
            'Offset - ' + offset + ', Response length - ',
            current_token_ownerships_v2?.length
          );

          result = result.concat(current_token_ownerships_v2);
          offset += current_token_ownerships_v2?.length;

          // Stop recursive call
          if (current_token_ownerships_v2?.length < limit)
            hasCollection = false;
        } else {
          retries--;

          if (retries == 0) {
            hasCollection = false;
            consoleLog('Maximum retries reached', response);
            throw new Error('Something went wrong. Please try again.');
          }
        }
      } catch (error) {
        retries--;

        if (retries == 0) {
          hasCollection = false;
          consoleLog('Maximum retries reached with error', error);
          throw new Error('Something went wrong. Please try again.');
        }
      }
    } while (hasCollection && retries > 0);

    consoleLog('Result on-chain: ', result?.length);

    // Also check current Aptos listings in our DB
    const listingResult = await getOwnerAptosNftListings(wallet);
    consoleLog('Result of listed NFTs: ', listingResult?.length);

    // Merge on-chain NFTs with listed NFTs
    result = [...result, ...listingResult];

    consoleLog('Result of on-chain + listed NFTs: ', result?.length);

    return extractAptosCollections(result);
  } catch (error) {
    errorLog('onAddAptosWallet Error', error);
    throw new Error('Something went wrong. Please try again.');
  }
};

// Extract correct fields from Aptos API endpoint
const extractAptosCollections = async (
  nftList: Array<any>
): Promise<{
  collectionList: WalletCollection[],
  galleryList: WalletGallery[],
}> => {
  const collectionList: WalletCollection[] = [];
  const galleryList: WalletGallery[] = [];

  if (nftList?.length > 0) {
    for (let i = 0; i < nftList.length; i++) {
      // If there's a marketplace field, the NFT is listed
      if (nftList[i]?.marketplace) {
        const {
          slug,
          collection_name: collectionName,
          name,
          image_url: image,
          token_data_id_hash: mintAddress,
        } = nftList[i];

        galleryList.push({
          slug,
          name: name || collectionName,
          image_url: image || '',
          mint_address: mintAddress,
          list_status: 'listed', // Any onchain APT NFTs in a user's wallet are unlisted
          price_multiplier: 1, // Default to 1
        });

        const nftIndex = findIndex(collectionList, ['slug', slug]);

        if (nftIndex >= 0) {
          // If Exists, increment owned_asset_count
          const newColData = cloneDeep(collectionList[nftIndex]);
          newColData.owned_asset_count++;
          collectionList[nftIndex] = newColData;
        } else {
          // If doesn't exist, create new entry
          collectionList.push({
            slug,
            name: collectionName,
            image_url: image || '',
            owned_asset_count: 1,
            hidden: false,
            manual_add: false,
            manual_owned_asset_count: 0,
            chain: 'aptos',
          });
        }
      } else {
        // on-chain NFT
        const {
          token_uri,
          current_collection,
          token_name: name,
          token_data_id: mintAddress,
        } = nftList[i]?.current_token_data;

        if (nftList[i]?.amount === 0) {
          consoleLog(name + ' has amount: 0, no longer owns NFT.');
          continue;
        }
        const collectionName = current_collection?.collection_name;

        // Fetch slug of collection via creator_address
        let slug = await getAptosCollectionSlug(
          current_collection?.creator_address
        );
        // For any unavailable NFTs
        if (slug == null) {
          slug =
            collectionName?.split(' ')?.join('_')?.toLowerCase() + '_aptos';
        }

        // Fetch NFT image from metadata URL
        const image =
          token_uri?.includes('.png') ||
            token_uri?.includes('.gif') ||
            token_uri?.includes('.jpg') ||
            token_uri?.includes('.jpeg')
            ? token_uri?.replace('ipfs://', 'https://ipfs.io/ipfs/')
            : await getNftImageFromMetadataUri(
              token_uri?.replace('ipfs://', 'https://ipfs.io/ipfs/'),
              3000
            );

        galleryList.push({
          slug,
          name: name || collectionName,
          image_url: image || '',
          mint_address: mintAddress,
          list_status: 'unlisted', // Any onchain APT NFTs in a user's wallet are unlisted
          price_multiplier: 1, // Default to 1
        });

        const nftIndex = findIndex(collectionList, ['slug', slug]);

        if (nftIndex >= 0) {
          // If Exists, increment owned_asset_count
          const newColData = cloneDeep(collectionList[nftIndex]);
          newColData.owned_asset_count++;
          collectionList[nftIndex] = newColData;
        } else {
          // If doesn't exist, create new entry
          collectionList.push({
            slug,
            name: collectionName,
            image_url: image || '',
            owned_asset_count: 1,
            hidden: false, // TO CHECK FOR THE VALUE
            manual_add: false,
            manual_owned_asset_count: 0,
            chain: 'aptos',
          });
        }
      }
    }
  }
  return {
    collectionList,
    galleryList,
  };
};

/**
 * Get owner's Aptos NFTs that are listed
 * @param {string} wallet owner's wallet address
 * @returns Listed Aptos NFTs of wallet address
 */
export const getOwnerAptosNftListings = async (wallet: string) => {
  return getCollection({
    query: {
      from_address: wallet,
    },
    collectionName: COLLECTION_NAMES.APTOS_LISTINGS,
  });
};
