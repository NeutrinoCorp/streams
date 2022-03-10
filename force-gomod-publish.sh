#!/bin/bash

while getopts ":m:v:" opt; do
  case $opt in
    m) MAJOR_VERSION=$OPTARG;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
        ;;
    v) VERSION_TAG=$OPTARG;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
        ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ "$MAJOR_VERSION" != "" ]
then
  MAJOR_VERSION="/v$MAJOR_VERSION"
fi

curl proxy.golang.org/github.com/neutrinocorp/streamhub"$MAJOR_VERSION"/@v/"$VERSION_TAG".info
