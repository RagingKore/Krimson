TARGET_PACKAGE=$1
NUGET_TARGET_SERVICE=$2
NUGET_API_KEY=$3

if [ "$TARGET_PACKAGE" = "Krimson" ]; then
  # Publish both the Krimson and the KrimsonTemplate packages
  TARGET_PACKAGES="$(find -wholename "./src/Krimson/**/*.nupkg")"
fi

if [ "$NUGET_TARGET_SERVICE" = "feedz" ]; then
  NUGET_TARGET_URL="https://f.feedz.io/Krimson/Krimson/nuget/index.json"
fi

for package in $(echo "$TARGET_PACKAGES" | grep "test" -v); do
  echo "${0##*/}": Pushing $package to $NUGET_TARGET_SERVICE \($NUGET_TARGET_URL\)...
  dotnet nuget push $package --source $NUGET_TARGET_URL --api-key $NUGET_API_KEY
done