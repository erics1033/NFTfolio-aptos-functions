# NFTfolio-aptos-functions

This repository contains the Aptos back-end functions used to aggregate and fetch NFT collections data.

Milestone One Release includes:

- Fetching onchain the floor price, usd floor price, unique number of owners, one day sales, one day volume, total supply and listed count for each supported Aptos NFT collection. In order to fetch these stats we fetch all events corresponding to an NFT collection onchain and store the active listings and sales history for each collection.
  
- Using the latest onchain NFT sales data to fetch new Aptos collections that are unsupported in the app and to automatically add support for them.

- Other functionality including displaying the charts, individual NFT project pages, adding Aptos collections to a user's watchlist, etc. are done on the front-end.

## Branching

Below is the environment breakdown:

- master

  > Production Branch. This is used to hold the latest stable version that is live in production.

- milestone-one

  > Branched out from master. This contains the latest changes for milestone one. All of these features have been merged into the master branch.
