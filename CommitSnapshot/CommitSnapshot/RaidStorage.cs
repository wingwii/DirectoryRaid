using System;
using System.Collections.Generic;
using System.IO;
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
        
        private string _virtualInputDir = null;
        private DirectoryRaid.StorageNode _virtualStorage = null;
        private DirectoryRaid.DirectoryNode _virtualRootDir = null;
        private List<DirectoryRaid.FileNode> _lstVirtualFiles = new List<DirectoryRaid.FileNode>();


        public RaidStorage(long blockSize, DirectoryRaid.StorageNode storage)
        {
            this._blockSize = blockSize;
            this._storage = storage;
        }

        public DirectoryRaid.Header RaidHeader { get; set; } = null;
        public List<DirectoryRaid.DirectoryNode> DirectoryNodes { get { return this._dirNodes; } }
        public List<DirectoryRaid.FileNode> FileNodes { get { return this._fileNodes; } }

        public DirectoryRaid.StorageNode Storage 
        { 
            get 
            { 
                if (null == this._storage)
                {
                    return this._virtualStorage;
                }
                else
                {
                    return this._storage;
                }
            }
        }

        public class RefInt64
        {
            public long value = 0;
        }

        public RefInt64 RefCurrentObjID { get; set; } = null;

        public void Start()
        {
            if (null == this._storage)
            {
                this.PrepareVirtualStorage();
            }
            else
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

        private void PrepareVirtualStorage()
        {
            this._virtualInputDir = Path.Combine(this.RaidHeader.RelativePath, this.RaidHeader.ID);

            this._virtualStorage = new DirectoryRaid.StorageNode();
            this._virtualStorage.ID = this.RefCurrentObjID.value;
            this._virtualStorage.NodeType = "S";
            this._virtualStorage.Name = this.RaidHeader.VolumeLabel;
            this._virtualStorage.SnapshotID = this.RaidHeader.ID;
            this._virtualStorage.StorageNumber = (int)this.RaidHeader.NumberOfPartitions + 1;
            this._virtualStorage.RelativePath = this._virtualInputDir;
            ++this.RefCurrentObjID.value;

            this._virtualRootDir = new DirectoryRaid.DirectoryNode();
            this._virtualStorage.RootDirectory = this._virtualRootDir;
            this._virtualRootDir.Storage = this._virtualStorage;
            this._virtualRootDir.ParentNode = this._virtualStorage;
            this._virtualRootDir.ID = this.RefCurrentObjID.value;
            this._virtualRootDir.NodeType = "D";
            this._virtualRootDir.Name = "/";
            ++this.RefCurrentObjID.value;

            ++this.RefCurrentObjID.value;

            this._dirNodes.Add(this._virtualRootDir);
            this._lstVirtualFiles.Add(this._virtualRootDir);
        }

        public void CommitVirtualStorage()
        {
            this._lstVirtualFiles.AddRange(this._virtualRootDir.Files);
            this._virtualStorage.AllFileNodes = this._lstVirtualFiles.ToArray();
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
            if (null == this._storage)
            {
                var file = new DirectoryRaid.FileNode();
                file.ID = this.RefCurrentObjID.value; 
                ++this.RefCurrentObjID.value;

                file.ParentNode = this._virtualRootDir;
                file.Storage = this._virtualStorage;
                file.Size = requestSize;
                file.NodeType = "F";
                file.Name = "p" + (0 + this._virtualRootDir.Files.Count).ToString("x");
                this._virtualRootDir.Files.Add(file);
                this._fileNodes.Add(file);

                var part = new FilePart();
                part.DataFile = file;
                part.PartNumber = 1;
                part.Offset = 0;
                part.Size = requestSize;
                results.Add(part);
            }
            else
            {
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
