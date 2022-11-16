#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <memory>

namespace node {
  typedef std::vector<std::string> SymbolFiles;
  typedef std::vector<std::string> Symbols;
  typedef uint_fast64_t FileId;
  const FileId NULL_FILEID = 0;


  enum class TYPE : char {
      NONE = 0,
      BID = 1,
      ASK = 2,
      TRADE = 3
  };

  const char* getTypeStr(TYPE tp) {
    switch(tp) {
    case TYPE::BID:
      return "BID";
    case TYPE::ASK:
      return "ASK";
    case TYPE::TRADE:
      return "TRADE";
    }
    return "NONE";
  }

  TYPE getType(const std::string& tp) {
    if (tp == "ASK")
      return TYPE::ASK;
    else if (tp == "BID")
      return TYPE::BID;
    else if (tp == "TRADE")
      return TYPE::TRADE;
    else
      return TYPE::NONE;
  }

  struct Node {
    FileId m_fId;
    int_fast64_t m_tm;
    double m_px;
    uint_fast64_t m_sz;
    std::string m_exch;
    TYPE m_type;
  };

  typedef std::shared_ptr<Node> NodePtr;
}
