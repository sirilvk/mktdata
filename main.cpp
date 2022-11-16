#include <node.h>
#include <filesystem>
#include <boost/assert.hpp>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <queue>
#include <set>


using namespace node;
namespace fs = std::filesystem;

const unsigned int NPROCS = 5; // use 5 threads to start with


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
};

// node on priority queue
struct PNode {
    volatile bool m_done = false;
    volatile bool m_consumed = false;
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

std::vector<std::unique_ptr<FileHandler>> fileHandles;

std::string getSymbol(const std::string& fname) {
    auto index = fname.find_last_of(".");
    if (std::string::npos == index) return fname;
    return fname.substr(0, index);
}

std::priority_queue<PNode, std::vector<PNode>, Compare> PQ;

void singleThProcess(const std::string& path) {
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
    for (auto i = 1; i < MAX_FILEID; ++i) {
	PNode pn;
	pn.m_node = fileHandles[i]->getNextNode();
	if (pn.m_node == nullptr) continue;
	PQ.push(pn);
    }

    std::cout << PQ.size() << std::endl;

    std::cout << "#Symbol,Timestamp,Price,Size,Exchange,Type" << std::endl;
    while(not PQ.empty()) {
	auto nd = PQ.top().m_node;
	std::cout << *nd << std::endl;
	PQ.pop();

	// get the next one for one that was removed
	PNode pn;
	pn.m_node = fileHandles[nd->m_fId]->getNextNode();
	if (pn.m_node)
	    PQ.push(pn);
    }
}
    

int main() {
    singleThProcess("/home/siril/prjs/bestex/mkt");
}
