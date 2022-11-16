mkdir -p cmake-build-debug
rm -rf cmake-build-debug/*
cd cmake-build-debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j
cd ..

mkdir -p cmake-build-release
rm -rf cmake-build-release/*
cd cmake-build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
cd ..
