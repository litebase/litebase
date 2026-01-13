#!/bin/bash

# Define variables
SQLITE_VERSION="3510100"
SQLITE_YEAR="2025"
SQLITE_URL="https://www.sqlite.org/${SQLITE_YEAR}/sqlite-amalgamation-${SQLITE_VERSION}.zip"
TEMP_DIR="/tmp/sqlite-update"
TARGET_DIR="./pkg/sqlite3"

# Create a temporary directory
mkdir -p $TEMP_DIR

# Download the SQLite amalgamation zip
echo "Downloading SQLite version $SQLITE_VERSION..."
curl -o $TEMP_DIR/sqlite.zip $SQLITE_URL

# Unzip the downloaded file
echo "Extracting SQLite files..."
unzip -o $TEMP_DIR/sqlite.zip -d $TEMP_DIR

# Replace the existing SQLite files
echo "Updating SQLite files in $TARGET_DIR..."
cp $TEMP_DIR/sqlite-amalgamation-${SQLITE_VERSION}/sqlite3.c $TARGET_DIR
cp $TEMP_DIR/sqlite-amalgamation-${SQLITE_VERSION}/sqlite3.h $TARGET_DIR

# Clean up
rm -rf $TEMP_DIR

echo "SQLite update process completed!"
