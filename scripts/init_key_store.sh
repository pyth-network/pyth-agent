#!/usr/bin/env bash
set -e

# This script initializes the keystore directory used by the agent binary.

KENV=$1 # The environment targeted
KDIR=$2 # The directory of the key store

case $KENV in
  mainnet)
    MAP_KEY=AHtgzX45WTKfkPG53L6WYhGEXwQkN1BVknET3sVsLL8J
    PGM_KEY=FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH
    ;;
  devnet)
    MAP_KEY=BmA9Z6FjioHJPpjT39QazZyhDRUdZy2ezwx4GiDdE2u2
    PGM_KEY=gSbePebfvPy7tRqimPoVecS2UsBvYv46ynrzWocc92s
    ;;
  testnet)
    MAP_KEY=AFmdnt9ng1uVxqCmqwQJDAYC5cKTkw8gJKSM5PnzuF6z
    PGM_KEY=8tfDNiaEyrV6Q1U4DEXrEigs9DoDtkugzFbybENEbCDz
    ;;
  localnet)
    MAP_KEY=BTJKZngp3vzeJiRmmT9PitQH4H29dhQZ1GNhxFfDi4kw
    PGM_KEY=BujGr9ChcuaCJhxeFEvGvaCFTxSV1CUCSVHL1SVFpU4i
    ;;
  *)
    echo "Unknown environment. Please use: mainnet, devnet, testnet, localnet"
    exit 1;
esac

if [ -z "$KDIR" ] ; then
  KDIR=$HOME/.pythd
fi

PKEY_FILE=$KDIR/publish_key_pair.json
if [ ! -f $PKEY_FILE ] ; then
  echo "cannot find $PKEY_FILE"
  exit 1
fi

echo $PGM_KEY > $KDIR/program_key.json
chmod 0400 $KDIR/program_key.json
echo $MAP_KEY > $KDIR/mapping_key.json
chmod 0400 $KDIR/mapping_key.json
chmod 0700 $KDIR
