using System;
using System.Collections.Generic;
using System.Text;

namespace DirectoryRaid
{
    public class MetaStorageParser
    {
        private long _baseID = 10;
        private long _newBaseID = -1;

        public MetaStorageParser()
        {
            //
        }

        public long NewBaseID
        {
            get
            {
                return (this._newBaseID + 1);
            }
        }

        public StorageNode Parse(string[] rows)
        {
            StorageNode result = null;
            var lstAllFileNodes = new List<Node>();
            var dicNodes = new Dictionary<long, Node>();
            foreach (var row in rows)
            {
                var cells = row.Split('|');
                var nCell = cells.Length;
                if (nCell < 2)
                {
                    continue;
                }

                Node node = null;
                var isFileNode = false;
                var nodeType = cells[0].ToUpper();
                var nodeID = long.Parse(cells[1]);
                if (nodeType.Equals("S", StringComparison.Ordinal))
                {
                    if (result != null)
                    {
                        continue;
                    }
                    if (nCell >= 9)
                    {
                        var storage = new StorageNode();
                        storage.SnapshotID = cells[4];
                        storage.StorageNumber = int.Parse(cells[5]);
                        storage.Size = long.Parse(cells[6]);
                        storage.Name = cells[7];
                        storage.RelativePath = cells[8];
                        node = storage;
                        if (null == result)
                        {
                            result = storage;
                        }
                    }
                }
                else if (false
                    || nodeType.Equals("D", StringComparison.Ordinal)
                    || nodeType.Equals("F", StringComparison.Ordinal)
                )
                {
                    if (nCell >= 7)
                    {
                        FileNode file = null;
                        if (nodeType.Equals("F", StringComparison.Ordinal))
                        {
                            file = new FileNode();
                        }
                        else
                        {
                            file = new DirectoryNode();
                        }

                        file.ParentNodeID = long.Parse(cells[2]);
                        file.Size = long.Parse(cells[3]);
                        file.CreationTime = long.Parse(cells[4], System.Globalization.NumberStyles.HexNumber);
                        file.LastWriteTime = long.Parse(cells[5], System.Globalization.NumberStyles.HexNumber);
                        file.Name = cells[6];
                        node = file;

                        if (file.ParentNodeID >= 0)
                        {
                            file.ParentNodeID += this._baseID;
                        }

                        isFileNode = true;
                    }
                }

                if (node != null)
                {
                    node.NodeType = nodeType;
                    node.ID = nodeID + this._baseID;
                    dicNodes[node.ID] = node;
                    this._newBaseID = (long)Math.Max(this._newBaseID, node.ID);

                    var file = node as FileNode;
                    if (file != null)
                    {
                        file.Storage = result;
                    }

                    if (isFileNode)
                    {
                        lstAllFileNodes.Add(node);
                    }
                }
            }
            
            if (null == result)
            {
                return null;
            }
            result.AllFileNodes = lstAllFileNodes.ToArray();

            foreach (var kv in dicNodes)
            {
                var nodeID = kv.Key;
                var node = kv.Value as FileNode;
                if (null == node)
                {
                    continue;
                }

                Node parentNode = null;
                if (!dicNodes.TryGetValue(node.ParentNodeID, out parentNode))
                {
                    parentNode = null;
                }

                node.ParentNode = parentNode;
                if (parentNode.ID == result.ID)
                {
                    result.RootDirectory = node;
                    node.Name = "/";
                }

                var parentDir = parentNode as DirectoryNode;
                if (parentDir != null)
                {
                    parentDir.Files.Add(node);
                }
            }
            if (null != result)
            {
                if (null == result.RootDirectory)
                {
                    result = null;
                }
            }
            if (null != result)
            {
                this._baseID = this._newBaseID + 1;
            }
            return result;
        }

        //
    }
}
