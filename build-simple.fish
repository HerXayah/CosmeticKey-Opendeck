#!/usr/bin/env fish

echo "Building plugin..."
cargo build --release --target x86_64-unknown-linux-gnu

if test $status -ne 0
    echo "Build failed!"
    exit 1
end

echo "Installing binary..."
mkdir -p x86_64-unknown-linux-gnu/bin
cp target/x86_64-unknown-linux-gnu/release/opendeck-cosmetickey x86_64-unknown-linux-gnu/bin/

echo "Removing build leftovers..."
rm -rf target

echo "✓ Done! Restart OpenDeck to load the plugin."
