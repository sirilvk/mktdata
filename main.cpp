#include <node.h>
#include <filesystem>
#include <boost/assert.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/scope_exit.hpp>
#include <fstream>
#include <queue>
#include <set>
#include <atomic>
#include <thread>


using namespace node;
namespace fs = std::filesystem;
namespace po = boost::program_options;

unsigned int NPROCS = 5; // use 5 threads to start with


SymbolFiles symFileList;
Symbols symbols;
std::unordered_map<std::string, FileId> fileIdMap;

std::ostream& operator<<(std::ostream& ostr, Node& nd) {
    auto tm = nd.m_tm / 1000;
    auto millis = nd.m_tm % 1000;
    std::tm ptm = *std::localtime(&tm);
    char dt[64], buffer[32];
    std::strftime(buffer, 32, "%Y-%m-%d %H:%M:%S", &ptm);
    snprintf(dt, 64, "%s.%03ld", buffer, millis);
    ostr << symbols[nd.m_fId] << "," << dt << "," << nd.m_px << "," << nd.m_sz << "," << nd.m_exch << "," << getTypeStr(nd.m_type);
    return ostr;
}

class FileHandler {
private:
    std::ifstream m_ifs;
    FileId m_fId = 0;

    uint_fast64_t getMillis(const std::string& tm) {
	auto indx = tm.find_last_of(".");
	if (indx == std::string::npos) return 0;
	return std::stoll(tm.substr(indx + 1));
    }

public:
    ~FileHandler() { m_ifs.close(); }
    FileHandler(const std::string& fname, const FileId fId):m_fId(fId) {
	// std::cout << "Opening " << fname << std::endl;
	m_ifs.open(fname);
    }

    NodePtr getNextNode() {
	if (not m_ifs.good()) return nullptr;
	std::string line;
	while(std::getline(m_ifs, line)) {
	    boost::algorithm::trim(line);
	    if (std::empty(line)) continue;
	    if (boost::algorithm::starts_with(line, "#")) continue;
	    if (boost::algorithm::starts_with(line, "//")) continue;

	    std::vector<std::string> args;
	    boost::split(args, line, boost::is_any_of(","));
	    BOOST_ASSERT(args.size() == 5);

	    auto node = std::make_shared<Node>();
	    std::tm mkTm = {};
	    strptime(args[0].c_str(), "%Y-%m-%d %H:%M:%S", &mkTm);

	    node->m_tm = (std::mktime(&mkTm) * 1000) + getMillis(args[0]);
	    node->m_px = std::stod(args[1]);
	    node->m_sz = std::stoll(args[2]);
	    node->m_exch = args[3];
	    node->m_type = getType(args[4]);
	    node->m_fId = m_fId;
	    return node;
	}
	return nullptr;
    }

    bool getNextNode(NodePtr& node) {
	if (not m_ifs.good()) return false;
	std::string line;
	while(std::getline(m_ifs, line)) {
	    boost::algorithm::trim(line);
	    if (std::empty(line)) continue;
	    if (boost::algorithm::starts_with(line, "#")) continue;
	    if (boost::algorithm::starts_with(line, "//")) continue;

	    std::vector<std::string> args;
	    boost::split(args, line, boost::is_any_of(","));
	    BOOST_ASSERT(args.size() == 5);

	    std::tm mkTm = {};
	    strptime(args[0].c_str(), "%Y-%m-%d %H:%M:%S", &mkTm);

	    node->m_tm = (std::mktime(&mkTm) * 1000) + getMillis(args[0]);
	    node->m_px = std::stod(args[1]);
	    node->m_sz = std::stoll(args[2]);
	    node->m_exch = args[3];
	    node->m_type = getType(args[4]);
	    node->m_fId = m_fId;
	    return true;
	}
	return false;
    }

};

// node on priority queue
struct PNode {
    NodePtr m_node;
    int m_indx = -1;
};

// node for multiple threads
struct TPNode {
    std::atomic<bool> m_available = ATOMIC_VAR_INIT(false);
    NodePtr m_node;
};

struct Compare {
    bool operator()(const PNode& left, const PNode& right) const {
	if (left.m_node == nullptr or right.m_node == nullptr) {
	    std::cout << "Invalid Condition!!" << std::endl;
	    return false;
	}

	if (left.m_node->m_tm == right.m_node->m_tm) {
	    return (left.m_node->m_fId > right.m_node->m_fId);
	} else {
	    return left.m_node->m_tm > right.m_node->m_tm;
	}
    }
};

typedef std::priority_queue<PNode, std::vector<PNode>, Compare> PRIQUE;

std::vector<std::unique_ptr<FileHandler>> fileHandles;

std::string getSymbol(const std::string& fname) {
    auto index = fname.find_last_of(".");
    if (std::string::npos == index) return fname;
    return fname.substr(0, index);
}

PRIQUE PQ;

FileId initialize(const std::string& path) {
    // NULL FileId placeholder
    symFileList.emplace_back(""); 
    symbols.emplace_back("");
    fileHandles.emplace_back(nullptr);
    FileId fId = NULL_FILEID;
    for (const auto & entry : fs::directory_iterator(path)) {
	if (fs::is_regular_file(entry) && entry.path().extension() == ".txt") {
	    const auto sym = getSymbol(entry.path().filename());
	    symFileList.emplace_back(entry.path());
	    symbols.emplace_back(sym);
	    fileIdMap.emplace(entry.path(), ++fId);
	    fileHandles.emplace_back(std::make_unique<FileHandler>(entry.path(), fId));
	}
    }
    const FileId MAX_FILEID = ++fId;
    BOOST_ASSERT(symbols.size() == MAX_FILEID);

    int j = 0;
    for (const auto& i : fileIdMap) {
	BOOST_ASSERT(i.first == symFileList[i.second]);
    }

    std::cout << "MAX FILE ID " << MAX_FILEID << std::endl;
    return MAX_FILEID;
}

void singleThProcess(const std::string& path, const std::string& ofile) {
    const FileId MAX_FILEID = initialize(path);
    for (auto i = 1; i < MAX_FILEID; ++i) {
	PNode pn;
	pn.m_node = fileHandles[i]->getNextNode();
	if (pn.m_node == nullptr) continue;
	PQ.push(pn);
    }

    std::cout << PQ.size() << std::endl;

    std::ofstream ofs(ofile);
    BOOST_SCOPE_EXIT(&ofs) {
	ofs.close();
    } BOOST_SCOPE_EXIT_END
    
    ofs << "#Symbol,Timestamp,Price,Size,Exchange,Type" << std::endl;
    while(not PQ.empty()) {
	auto nd = PQ.top().m_node;
	ofs << *nd << std::endl;
	PQ.pop();

	// get the next one for one that was removed
	PNode pn;
	if (fileHandles[nd->m_fId]->getNextNode(nd)) {
	    pn.m_node = nd;
	    if (pn.m_node)
		PQ.push(pn);
	}
    }
}

void multiThProcess(const std::string& path, const std::string& ofile) {
    const FileId MAX_FILEID = initialize(path);
    TPNode aggNodes[NPROCS];

    // worker threads havin their own PWs will handle their set of symbol files in parallel
    auto workerFn = [&](const FileId start, const FileId end, const unsigned int currNode) {
	PRIQUE workerPQ;
	auto& cn = aggNodes[currNode];

	for (auto i = start; i < end; ++i) {
	    PNode pn;
	    pn.m_node = fileHandles[i]->getNextNode();
	    if (pn.m_node == nullptr) continue;
	    workerPQ.push(pn);
	}
	std::cout << "Thread id " << currNode << " PQ size=" << workerPQ.size() << " start=" << start << " end=" << end << std::endl;

	while(not workerPQ.empty()) {
	    NodePtr nd = workerPQ.top().m_node;
	    workerPQ.pop();

	    // insert into aggNodes (cn)
	    while(cn.m_available.load(std::memory_order_acquire)) {}
	    cn.m_node = nd;
	    cn.m_available.store(true, std::memory_order_release);

	    // get the next one for one that was removed.
	    PNode pn;
	    pn.m_indx = currNode;
	    pn.m_node = fileHandles[nd->m_fId]->getNextNode();
	    if (pn.m_node)
		workerPQ.push(pn);
	}

	// done
	while(cn.m_available.load(std::memory_order_acquire)) {}
	cn.m_node = nullptr;
	cn.m_available.store(true, std::memory_order_release);
    };

    const auto wkrLd = (MAX_FILEID - 1) / NPROCS;
    auto wkrSt = 1;
    for (int i = 0; i < NPROCS; ++i) {
	auto wkrEnd = wkrSt + wkrLd;
	if (i == 0) wkrEnd += (MAX_FILEID - 1) % NPROCS;
	if (wkrEnd > MAX_FILEID) wkrEnd = MAX_FILEID;
	std::thread wkr(workerFn, wkrSt, wkrEnd, i);
	wkrSt = wkrEnd;
	wkr.detach();
    }

    PRIQUE primaryPQ;
    // load from all threads
    for (int i = 0; i < NPROCS; ++i) {
	auto& cn = aggNodes[i];

	PNode tmp;
	tmp.m_indx = i;
	while(not cn.m_available.load(std::memory_order_acquire)) { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
	tmp.m_node = cn.m_node;
	cn.m_available.store(false, std::memory_order_release);
	primaryPQ.push(tmp);
    }

    std::ofstream ofs(ofile);
    BOOST_SCOPE_EXIT(&ofs) {
	ofs.close();
    } BOOST_SCOPE_EXIT_END
    
    ofs << "#Symbol,Timestamp,Price,Size,Exchange,Type" << std::endl;
    // aggregator (current main thread) will handle the update coming from workers
    while(not primaryPQ.empty()) {
	const PNode& pn = primaryPQ.top();
	NodePtr nd = pn.m_node;
	ofs << *nd << std::endl;
	BOOST_ASSERT(pn.m_indx != -1);

	PNode newNd;
	newNd.m_indx = pn.m_indx;
	auto& cn = aggNodes[newNd.m_indx];
	primaryPQ.pop();

	while(not cn.m_available.load(std::memory_order_acquire)) {}
	newNd.m_node = cn.m_node;
	cn.m_available.store(false, std::memory_order_release);

	// get the next one for one that was removed.
	if (newNd.m_node)
	    primaryPQ.push(newNd);
    }
}
	  
int main(int argc, char** argv) {
    std::string inDir, outFile;
    bool isMultithreaded = false;

    po::options_description desc("MktData Options");
    desc.add_options ()
	("help,h", "print usage message")
	("input,i", po::value(&inDir), "Input directory path")
	("output,o", po::value(&outFile), "Output file")
	("mt,m", po::value(&isMultithreaded)->implicit_value(true), "run multithreaded")
	("threads,t", po::value(&NPROCS), "thread count");

    // Parse command line arguments
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
    po::notify(vm);

    if (vm.count("help") || std::empty(inDir) || std::empty(outFile)) {
	std::cout << desc << std::endl;
	return 0;
    }

    auto nw = std::chrono::high_resolution_clock::now();
    if (isMultithreaded) {
	std::cout << "Running in multithreaded mode with thread count = " << NPROCS << std::endl;
	multiThProcess(inDir, outFile);
    } else {
	std::cout << "Running in single threaded mode." << std::endl;
	singleThProcess(inDir, outFile);
    }
    std::cout << "Total time taken " << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - nw).count() << std::endl;
}
