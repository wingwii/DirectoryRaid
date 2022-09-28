using System;
using System.Collections.Generic;
using System.Text;

namespace CommitSnapshot
{
    class RaidStorage
    {
        private long _blockSize = 0;
        private DirectoryRaid.StorageNode _storage = null;
        private List<DirectoryRaid.DirectoryNode> _dirNodes = new List<DirectoryRaid.DirectoryNode>();
        private List<DirectoryRaid.FileNode> _fileNodes = new List<DirectoryRaid.FileNode>();
        private List<FileSplitter> _fileSplitter = new List<FileSplitter>();
        private int _currentFileIdx = 0;


        public RaidStorage(long blockSize, DirectoryRaid.StorageNode storage)
        {
            this._blockSize = blockSize;
            this._storage = storage;
        }

        public DirectoryRaid.StorageNode Storage { get { return this._storage; } }
        public List<DirectoryRaid.DirectoryNode> DirectoryNodes { get { return this._dirNodes; } }
        public List<DirectoryRaid.FileNode> FileNodes { get { return this._fileNodes; } }


        public void Start()
        {
            var nodes = this._storage.AllFileNodes;
            foreach (var node in nodes)
            {
                var file = node as DirectoryRaid.FileNode;
                if (file != null)
                {
                    if (file.IsFile)
                    {
                        this._fileNodes.Add(file);

                        var fs = new FileSplitter(file);
                        this._fileSplitter.Add(fs);
                    }
                    else
                    {
                        this._dirNodes.Add(file as DirectoryRaid.DirectoryNode);
                    }
                }
            }
            //
        }

        public class FilePart
        {
            public int PartNumber { get; set; } = 0;
            public DirectoryRaid.FileNode DataFile { get; set; } = null;
            public long Offset { get; set; } = 0;
            public long Size { get; set; } = 0;

            public override string ToString()
            {
                var s = "[" + this.PartNumber.ToString() + ":" + this.Offset.ToString() + ":" + this.Size.ToString() + "] ";
                s += this.DataFile.Name;
                return s;
            }
        }


        private class FileSplitter
        {
            private DirectoryRaid.FileNode _file = null;
            private long _size = 0;
            private long _offset = 0;

            public FileSplitter(DirectoryRaid.FileNode file)
            {
                this._file = file;
                this._size = file.Size;
            }

            public DirectoryRaid.FileNode DataFile { get { return this._file; } }

            public FilePart Next(long blockSize)
            {
                if (this._offset >= this._size)
                {
                    return null;
                }

                var actualSize = this._size - this._offset;
                if (actualSize > blockSize)
                {
                    actualSize = blockSize;
                }

                var result = new FilePart();
                result.DataFile = this._file;
                result.Offset = this._offset;
                result.Size = actualSize;

                this._offset += actualSize;
                return result;
            }
            //
        }

        public FilePart[] Next()
        {
            var results = new List<FilePart>();
            var requestSize = this._blockSize;
            while (requestSize > 0)
            {
                if (this._currentFileIdx >= this._fileSplitter.Count)
                {
                    break;
                }

                var fs = this._fileSplitter[this._currentFileIdx];
                var part = fs.Next(requestSize);
                if (null == part)
                {
                    ++this._currentFileIdx;
                    continue;
                }

                results.Add(part);
                part.PartNumber = results.Count;
                requestSize -= part.Size;
            }
            if (results.Count <= 0)
            {
                return null;
            }
            return results.ToArray();
        }

        //
    }
}
