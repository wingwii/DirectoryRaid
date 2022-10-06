using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace DirectoryRaid
{
    public class RaidDB
    {
        private List<StorageNode> _lstStorages = new List<StorageNode>();
        private Dictionary<int, StorageNode> _dicStoragesByNum = new Dictionary<int, StorageNode>();
        private Dictionary<long, Node> _dicNodes = new Dictionary<long, Node>();
        private Dictionary<long, DataBlock> _dicDataBlocks = new Dictionary<long, DataBlock>();
        private Dictionary<long, FilePartsGroup> _dicFilePartsGroups = new Dictionary<long, FilePartsGroup>();
        private List<FilePart> _lstFileParts = new List<FilePart>();

        private StorageNode[] _arStorages = null;
        private DataBlock[] _arDatBlocks = null;


        public RaidDB()
        {
            //
        }

        public StorageNode[] Storages { get { return this._arStorages; } }
        public DataBlock[] DataBlocks { get { return this._arDatBlocks; } }
        public DirectoryRaid.Header RaidHeader { get; set; } = null;
        public string FileName { get; set; } = string.Empty;

        
        public FileNode FindFileByID(long nodeID)
        {
            Node node = null;
            if (this._dicNodes.TryGetValue(nodeID, out node))
            {
                return node as FileNode;
            }
            else
            {
                return null;
            }
        }

        public bool Load()
        {
            var result = true;

            var fileName = this.FileName;

            this._arStorages = null;
            this._arDatBlocks = null;

            this._lstStorages.Clear();
            this._dicStoragesByNum.Clear();
            this._dicNodes.Clear();
            this._dicDataBlocks.Clear();
            this._dicFilePartsGroups.Clear();
            this._lstFileParts.Clear();

            long maxObjID = -1;
            var fs = File.OpenRead(fileName);
            var reader = new BinaryReader(fs);
            while (fs.Position < fs.Length)
            {
                var nodeType = (uint)reader.ReadByte();
                var nodeID = ReadID(reader);
                maxObjID = (long)Math.Max(maxObjID, nodeID);
                if (1 == nodeType)
                {
                    var storage = new StorageNode();
                    storage.NodeType = "S";
                    storage.ID = nodeID;
                    storage.StorageNumber = reader.ReadUInt16();
                    storage.Size = reader.ReadInt64();
                    storage.Name = ReadStringW(reader);
                    storage.RelativePath = ReadStringW(reader);

                    this._lstStorages.Add(storage);
                    this._dicNodes[nodeID] = storage;
                    this._dicStoragesByNum[storage.StorageNumber] = storage;
                }
                else if (2 == nodeType || 3 == nodeType)
                {
                    var file = (FileNode)null;
                    if (2 == nodeType)
                    {
                        file = new DirectoryNode();
                        file.NodeType = "D";
                    }
                    else
                    {
                        file = new FileNode();
                        file.NodeType = "F";
                    }

                    file.ID = nodeID;
                    file.ParentID = ReadID(reader);
                    file.StorageID = ReadID(reader);

                    file.Size = reader.ReadInt64();
                    file.CreationTime = reader.ReadInt64();
                    file.LastWriteTime = reader.ReadInt64();
                    file.Name = ReadStringW(reader);

                    this._dicNodes[nodeID] = file;
                }
                else if (16 == nodeType)
                {
                    var block = new DataBlock();
                    block.ID = nodeID;
                    block.BlockNumber = reader.ReadInt32();
                    block.Size = (long)reader.ReadUInt32();
                    this._dicDataBlocks[nodeID] = block;
                }
                else if (17 == nodeType)
                {
                    var grp = new FilePartsGroup();
                    grp.ID = nodeID;

                    grp.BlockID = ReadID(reader);
                    grp.StorageNumber = (int)reader.ReadUInt16();

                    grp.OffsetToDataHash = fs.Position;
                    var hashExisted = reader.ReadByte();
                    grp.DataHash = reader.ReadBytes(32);
                    if (0 == hashExisted)
                    {
                        grp.DataHash = null;
                    }

                    this._dicFilePartsGroups[nodeID] = grp;
                }
                else if (18 == nodeType)
                {
                    var part = new FilePart();
                    part.ID = nodeID;

                    part.GroupID = ReadID(reader);
                    part.FileID = ReadID(reader);
                    part.PartNumber = reader.ReadInt32();
                    part.Offset = (long)reader.ReadUInt32();
                    part.Size = (long)reader.ReadUInt32();

                    this._lstFileParts.Add(part);
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
                this.LinkObjects();
            }

            return result;
        }

        private void LinkObjects()
        {
            foreach (var kv in this._dicNodes)
            {
                var node = kv.Value;
                var file = node as FileNode;
                if (file != null)
                {
                    Node parent = null;
                    this._dicNodes.TryGetValue(file.ParentID, out parent);
                    file.Parent = parent;

                    {
                        Node storage = null;
                        this._dicNodes.TryGetValue(file.StorageID, out storage);
                        file.Storage = storage;
                    }
                    {
                        var parentDir = parent as DirectoryNode;
                        if (parentDir != null)
                        {
                            parentDir.ChildNodes.Add(file);
                        }
                    }

                    if (!file.IsFile)
                    {
                        var storage = parent as StorageNode;
                        if (storage != null)
                        {
                            storage.RootDirectory = node;
                        }
                    }
                }
            }

            var storageCount = this._lstStorages.Count;
            foreach (var kv in this._dicFilePartsGroups)
            {
                var grp = kv.Value;
                var storageNum = grp.StorageNumber;

                var block = this._dicDataBlocks[grp.BlockID];
                grp.DBlock = block;
                grp.Storage = this._dicStoragesByNum[storageNum];

                if (null == block.DGroups)
                {
                    block.DGroups = new FilePartsGroup[storageCount];
                }
                block.DGroups[storageNum - 1] = grp;
            }

            foreach (var part in this._lstFileParts)
            {
                var grp = this._dicFilePartsGroups[part.GroupID];
                part.Group = grp;
                part.DataFile = (FileNode)this._dicNodes[part.FileID];

                var partNum = part.PartNumber;
                var partIdx = partNum - 1;
                var lstParts = grp.Parts;
                while (lstParts.Count <= partIdx)
                {
                    lstParts.Add(null);
                }
                lstParts[partIdx] = part;
            }

            this._arStorages = new StorageNode[storageCount];
            foreach (var storage in this._lstStorages)
            {
                this._arStorages[storage.StorageNumber - 1] = storage;
            }

            this._arDatBlocks = new DataBlock[this._dicDataBlocks.Count];
            foreach (var kv in this._dicDataBlocks)
            {
                var block = kv.Value;
                this._arDatBlocks[block.BlockNumber - 1] = block;
            }
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

        public class Node
        {
            public long ID = 0;
            public string NodeType = string.Empty;
            public long Size = 0;
        }

        public class NamedNode : Node
        {
            public string Name = string.Empty;

            public override string ToString()
            {
                return this.Name;
            }
        }

        public class StorageNode : NamedNode
        {
            public int StorageNumber = 0;
            public string RelativePath = string.Empty;
            public Node RootDirectory = null;
        }

        public class FileNode : NamedNode
        {
            public long StorageID = -1;
            public long ParentID = -1;

            public Node Storage = null;
            public Node Parent = null;

            public long CreationTime = 0;
            public long LastWriteTime = 0;

            public virtual bool IsFile { get { return true; } }
        }

        public class DirectoryNode : FileNode
        {
            public List<FileNode> ChildNodes = new List<FileNode>();
            public override bool IsFile { get { return false; } }
        }

        public class DataBlock : Node
        {
            public int BlockNumber = 0;

            public FilePartsGroup[] DGroups = null;
        }

        public class FilePartsGroup : Node
        {
            public long OffsetToDataHash = 0;
            public byte[] DataHash = null;

            public long BlockID = -1;
            public int StorageNumber = 0;

            public DataBlock DBlock = null;
            public StorageNode Storage = null;

            public List<FilePart> Parts = new List<FilePart>();
        }

        public class FilePart : Node
        {
            public long GroupID = -1;
            public long FileID = -1;
            public int PartNumber = 0;
            public long Offset = 0;

            public FilePartsGroup Group = null;
            public FileNode DataFile = null;
        }
        //
    }
}
