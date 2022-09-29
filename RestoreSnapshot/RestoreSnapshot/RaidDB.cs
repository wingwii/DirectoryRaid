using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace RestoreSnapshot
{
    class RaidDB
    {
        private Dictionary<int, StorageNodeWrp> _dicStoragesByNum = new Dictionary<int, StorageNodeWrp>();
        private Dictionary<long, StorageNodeWrp> _dicStorages = new Dictionary<long, StorageNodeWrp>();
        private Dictionary<long, FileNodeWrp> _dicFiles = new Dictionary<long, FileNodeWrp>();
        private Dictionary<long, DataBlockWrp> _dicDataBlocks = new Dictionary<long, DataBlockWrp>();
        private Dictionary<long, FilePartsGroupWrp> _dicFilePartsGroups = new Dictionary<long, FilePartsGroupWrp>();
        private Dictionary<long, FilePartWrp> _dicFileParts = new Dictionary<long, FilePartWrp>();

        private DirectoryRaid.StorageNode[] _storages = null;
        private DirectoryRaid.RaidDataBlock[] _dataBlocks = null;


        public RaidDB()
        {
            //
        }

        public string DbFileName { get; private set; } = string.Empty;

        public DirectoryRaid.StorageNode[] Storages
        {
            get { return this._storages; }
        }

        public DirectoryRaid.RaidDataBlock[] DataBlocks
        {
            get { return this._dataBlocks; }
        }

        public bool Load(string fileName)
        {
            var result = true;

            this.DbFileName = fileName;

            this._storages = null;
            this._dataBlocks = null;

            this._dicStorages.Clear();
            this._dicFiles.Clear();
            this._dicDataBlocks.Clear();
            this._dicFilePartsGroups.Clear();
            this._dicFileParts.Clear();

            var fs = File.OpenRead(fileName);
            var reader = new BinaryReader(fs);
            while (fs.Position < fs.Length)
            {
                var nodeType = (uint)reader.ReadByte();
                var nodeID = ReadID(reader);
                if (1 == nodeType)
                {
                    var storage = new DirectoryRaid.StorageNode();
                    storage.NodeType = "S";
                    storage.ID = nodeID;
                    storage.StorageNumber = reader.ReadUInt16();
                    storage.Size = reader.ReadInt64();
                    storage.Name = ReadStringW(reader);
                    storage.RelativePath = ReadStringW(reader);

                    var wrp = new StorageNodeWrp();
                    wrp.target = storage;
                    this._dicStorages[nodeID] = wrp;
                    this._dicStoragesByNum[storage.StorageNumber] = wrp;
                }
                else if (2 == nodeType || 3 == nodeType)
                {
                    var file = (DirectoryRaid.FileNode)null;
                    if (2 == nodeType)
                    {
                        file = new DirectoryRaid.DirectoryNode();
                        file.NodeType = "D";
                    }
                    else
                    {
                        file = new DirectoryRaid.FileNode();
                        file.NodeType = "F";
                    }

                    file.ID = nodeID;
                    var parentNodeID = ReadID(reader);
                    var storageID = ReadID(reader);

                    file.Size = reader.ReadInt64();
                    file.CreationTime = reader.ReadInt64();
                    file.LastWriteTime = reader.ReadInt64();
                    file.Name = ReadStringW(reader);

                    var wrp = new FileNodeWrp();
                    wrp.target = file;
                    wrp.storageID = storageID;
                    wrp.parentNodeID = parentNodeID;
                    this._dicFiles[nodeID] = wrp;
                }
                else if (16 == nodeType)
                {
                    var block = new DirectoryRaid.RaidDataBlock();
                    block.ID = nodeID;
                    block.BlockNumber = reader.ReadInt32();
                    block.Size = (long)reader.ReadUInt32();

                    block.OffsetToHashData = fs.Position;
                    fs.Position += 32;

                    var wrp = new DataBlockWrp();
                    wrp.target = block;
                    this._dicDataBlocks[nodeID] = wrp;
                }
                else if (17 == nodeType)
                {
                    var grp = new DirectoryRaid.FilePartsGroup();
                    grp.ID = nodeID;

                    var blockID = ReadID(reader);
                    var storageNumber = (int)reader.ReadUInt16();

                    var wrp = new FilePartsGroupWrp();
                    wrp.target = grp;
                    wrp.blockID = blockID;
                    wrp.storageNumber = storageNumber;
                    this._dicFilePartsGroups[nodeID] = wrp;
                }
                else if (18 == nodeType)
                {
                    var part = new DirectoryRaid.FilePart();
                    part.ID = nodeID;

                    var groupID = ReadID(reader);
                    var fileID = ReadID(reader);
                    part.PartNumber = reader.ReadInt32();
                    part.Offset = (long)reader.ReadUInt32();
                    part.Size = (long)reader.ReadUInt32();

                    var wrp = new FilePartWrp();
                    wrp.target = part;
                    wrp.groupID = groupID;
                    wrp.fileID = fileID;
                    this._dicFileParts[nodeID] = wrp;
                }
                else
                {
                    result = false;
                    break;
                }
            }
            reader.Close();
            fs.Close();

            if (result)
            {
                result = false;
                if (this.ResolveAssocObjects())
                {
                    result = this.ConvertResults();
                }
            }

            return result;
        }

        private long ReadID(BinaryReader reader)
        {
            var result = (long)reader.ReadInt32();
            if (result >= 0xffffff)
            {
                reader.BaseStream.Position -= 4;
                result = reader.ReadInt64();
            }
            return result;
        }

        private byte[] ReadHdrAndBuf(BinaryReader reader)
        {
            var n = (int)reader.ReadUInt16();
            return reader.ReadBytes(n);
        }

        private string ReadStringW(BinaryReader reader)
        {
            var buf = ReadHdrAndBuf(reader);
            return Encoding.Unicode.GetString(buf);
        }

        private class StorageNodeWrp
        {
            public DirectoryRaid.StorageNode target = null;
        }

        private class FileNodeWrp
        {
            public DirectoryRaid.FileNode target = null;
            public long storageID = -1;
            public long parentNodeID = -1;
        }

        private class DataBlockWrp
        {
            public DirectoryRaid.RaidDataBlock target = null;
            public List<DirectoryRaid.FilePartsGroup> lstGrp = new List<DirectoryRaid.FilePartsGroup>();
        }

        private class FilePartsGroupWrp
        {
            public DirectoryRaid.FilePartsGroup target = null;
            public List<DirectoryRaid.FilePart> lstPart = new List<DirectoryRaid.FilePart>();
            public long blockID = -1;
            public int storageNumber = -1;
        }

        private class FilePartWrp
        {
            public DirectoryRaid.FilePart target = null;
            public long groupID = 0;
            public long fileID = 0;
        }

        private bool ConvertResults()
        {
            int i = 0;
            this._storages = new DirectoryRaid.StorageNode[this._dicStorages.Count];
            foreach (var kv in this._dicStorages)
            {
                this._storages[i++] = kv.Value.target;
            }

            return true;
        }

        private bool ResolveAssocObjects()
        {
            foreach (var kv in this._dicFiles)
            {
                var fileWrp = kv.Value;
                var file = fileWrp.target;
                var parentNodeID = fileWrp.parentNodeID;

                FileNodeWrp parentDirWrp = null;
                if (this._dicFiles.TryGetValue(parentNodeID, out parentDirWrp))
                {
                    var parentDir = parentDirWrp.target as DirectoryRaid.DirectoryNode;
                    file.ParentNode = parentDir;
                    parentDir.Files.Add(file);
                }
                else
                {
                    StorageNodeWrp storageWrp = null;
                    if (this._dicStorages.TryGetValue(parentNodeID, out storageWrp))
                    {
                        var storage = storageWrp.target;
                        storage.RootDirectory = file;
                    }
                }
            }

            foreach (var kv in this._dicFileParts)
            {
                var filePartWrp = kv.Value;
                var part = filePartWrp.target;

                var fileWrp = this._dicFiles[filePartWrp.fileID];
                part.DataFile = fileWrp.target;

                var grpWrp = this._dicFilePartsGroups[filePartWrp.groupID];
                grpWrp.lstPart.Add(part);
            }

            foreach (var kv in this._dicFilePartsGroups)
            {
                var grpWrp = kv.Value;
                var grp = grpWrp.target;
                grp.Storage = this._dicStoragesByNum[grpWrp.storageNumber].target;
                grp.Items = grpWrp.lstPart.ToArray();

                var blckWrp = this._dicDataBlocks[grpWrp.blockID];
                blckWrp.lstGrp.Add(grp);
            }

            int i = 0;
            this._dataBlocks = new DirectoryRaid.RaidDataBlock[this._dicDataBlocks.Count];
            foreach (var kv in this._dicDataBlocks)
            {
                var blckWrp = kv.Value;
                var blck = blckWrp.target;
                blck.Items = blckWrp.lstGrp.ToArray();
                this._dataBlocks[i++] = blck;
            }

            return true;
        }


        //
    }
}
