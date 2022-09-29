using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CommitSnapshot
{
    class BinFileExport
    {
        private RaidStorage[] _raidArray = null;
        private List<DirectoryRaid.RaidDataBlock> _results = null;
        private BinaryWriter _writer = null;

        public BinFileExport(RaidStorage[] raidArr, List<DirectoryRaid.RaidDataBlock> results)
        {
            this._raidArray = raidArr;
            this._results = results;
        }

        public void SaveAs(string fileName)
        {
            var fs = File.Create(fileName);
            var writer = new BinaryWriter(fs);
            this._writer = writer;

            foreach (var rs in this._raidArray)
            {
                var storage = rs.Storage;
                writer.Write((byte)1);
                this.WriteID(storage.ID);
                writer.Write((UInt16)storage.StorageNumber);
                writer.Write((UInt64)storage.Size);
                this.WriteStringW(storage.Name);
                this.WriteStringW(storage.RelativePath);

                var nodes = storage.AllFileNodes;
                foreach (var node in nodes)
                {
                    var file = node as DirectoryRaid.FileNode;
                    var nodeTypeName = file.NodeType;

                    var nodeType = 2;
                    if (nodeTypeName.Equals("F", StringComparison.OrdinalIgnoreCase))
                    {
                        nodeType = 3;
                    }

                    writer.Write((byte)nodeType);
                    this.WriteID(file.ID);
                    this.WriteID(GetParentNodeID(file));
                    this.WriteID(file.Storage.ID);
                    writer.Write((UInt64)file.Size);
                    writer.Write((Int64)file.CreationTime);
                    writer.Write((Int64)file.LastWriteTime);
                    this.WriteStringW(file.Name);
                }
            }

            foreach (var block in this._results)
            {
                writer.Write((byte)16);
                this.WriteID(block.ID);
                writer.Write((Int32)block.BlockNumber);
                writer.Write((UInt32)block.Size);

                // SHA-256
                writer.Write((Int64)0);
                writer.Write((Int64)0);
                writer.Write((Int64)0);
                writer.Write((Int64)0);
                
                foreach (var group in block.Items)
                {
                    if (null == group)
                    {
                        continue;
                    }

                    writer.Write((byte)17);
                    this.WriteID(group.ID);
                    this.WriteID(block.ID);
                    writer.Write((UInt16)group.Storage.StorageNumber);

                    if (null != group.Items)
                    {
                        foreach (var part in group.Items)
                        {
                            writer.Write((byte)18);
                            this.WriteID(part.ID);
                            this.WriteID(group.ID);
                            this.WriteID(part.DataFile.ID);
                            writer.Write((UInt32)part.PartNumber);
                            writer.Write((UInt32)part.Offset);
                            writer.Write((UInt32)part.Size);
                        }
                    }
                }
            }

            fs.Flush();
            writer.Flush();
            fs.Close();
            writer.Close();
            fs.Dispose();
            writer.Dispose();
        }

        public static long GetParentNodeID(DirectoryRaid.FileNode node)
        {
            var parentNode = node.ParentNode;
            if (null == parentNode)
            {
                return -1;
            }
            else
            {
                return parentNode.ID;
            }
        }

        private void WriteID(long id)
        {
            var writer = this._writer;
            if (id < 0xffffff)
            {
                writer.Write((UInt32)id);
            }
            else
            {
                writer.Write((Int64)id);
            }
        }

        private void WriteHdrAndBuf(byte[] buf)
        {
            var n = buf.Length;
            var writer = this._writer;
            writer.Write((UInt16)n);
            writer.Write(buf, 0, n);
        }

        private void WriteStringW(string s)
        {
            var buf = Encoding.Unicode.GetBytes(s);
            this.WriteHdrAndBuf(buf);
        }
        //
    }
}
