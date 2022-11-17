# mktdata

mktgen/gen.sh will generate market data for symbols from file symfile.txt (200 symbols). It can generate records as many as count parameter passed to the python script.

main.cpp has the main code which uses a priority queue to order the market data from the different files into one single file.

@TODO : Object pooling in multithreaded implementation
