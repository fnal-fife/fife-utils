#include <iostream>
#include <cassert>


/// IFDH interface (data handling)
namespace ifdh_ns {
  class ifdh;
}

namespace ana
{
  /// Fetch files from a pre-existing SAM project
  class SAMProjectSource
  {
  public:
    /// \param proj      SAM project name (not URL)
    /// \param fileLimit Optional maximum number of files to process
    SAMProjectSource(const std::string& proj, int fileLimit = -1);
    virtual ~SAMProjectSource();

    virtual TFile* GetNextFile();

    int NFiles() const {return fNFiles;}
  protected:
    ifdh_ns::ifdh *fIFDH;

    std::string fProjectURL;
    std::string fProcessID;

    TFile* fFile; ///< The most-recently-returned file

    int fNFiles;
  };
}
