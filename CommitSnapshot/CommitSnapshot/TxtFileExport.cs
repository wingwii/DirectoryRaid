using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CommitSnapshot
{
    class TxtFileExport
    {
        private RaidStorage[] _raidArray = null;
        private List<DirectoryRaid.RaidDataBlock> _results = null;
        private StreamWriter _writer = null;

        public TxtFileExport(RaidStorage[] raidArr, List<DirectoryRaid.RaidDataBlock> results)
        {
            this._raidArray = raidArr;
            this._results = results;
        }

        public void SaveAs(string fileName)
        {
            var writer = File.CreateText(fileName);
            this._writer = writer;

            foreach (var rs in this._raidArray)
            {
                var storage = rs.Storage;
                var sb = new StringBuilder();
                sb.Append(storage.NodeType);
                sb.Append('|');
                sb.Append(storage.ID.ToString("x"));
                sb.Append('|');
                sb.Append(storage.StorageNumber.ToString());
                sb.Append('|');
                sb.Append(storage.Size.ToString());
                sb.Append('|');
                sb.Append(storage.Name);
                sb.Append('|');
                sb.Append(storage.RelativePath);
                writer.WriteLine(sb.ToString());
            }

            foreach (var rs in this._raidArray)
            {
                var nodes = rs.DirectoryNodes;
                foreach (var node in nodes)
                {
                    this.WriteFileNodeInfoAsText(node);
                }
            }
            foreach (var rb in this._raidArray)
            {
                var nodes = rb.FileNodes;
                foreach (var node in nodes)
                {
                    this.WriteFileNodeInfoAsText(node);
                }
            }

            foreach (var block in this._results)
            {
                var sb = new StringBuilder();
                sb.Append("BA");
                sb.Append('|');
                sb.Append(block.ID.ToString("x"));
                sb.Append('|');
                sb.Append(block.BlockNumber.ToString("x"));
                sb.Append('|');
                sb.Append(block.Size.ToString("x"));
                writer.WriteLine(sb.ToString());

                foreach (var group in block.Items)
                {
                    if (null == group)
                    {
                        continue;
                    }

                    sb = new StringBuilder();
                    sb.Append(" BG");
                    sb.Append('|');
                    sb.Append(group.ID.ToString("x"));
                    sb.Append('|');
                    sb.Append(block.ID.ToString("x"));
                    sb.Append('|');
                    sb.Append(group.Storage.StorageNumber.ToString());
                    writer.WriteLine(sb.ToString());

                    if (null != group.Items)
                    {
                        foreach (var part in group.Items)
                        {
                            sb = new StringBuilder();
                            sb.Append("  BP");
                            sb.Append('|');
                            sb.Append(part.ID.ToString("x"));
                            sb.Append('|');
                            sb.Append(group.ID.ToString("x"));
                            sb.Append('|');
                            sb.Append(part.PartNumber.ToString("x"));
                            sb.Append('|');
                            sb.Append(part.DataFile.ID.ToString("x"));
                            sb.Append('|');
                            sb.Append(part.Offset.ToString("x"));
                            sb.Append('|');
                            sb.Append(part.Size.ToString("x"));
                            writer.WriteLine(sb.ToString());
                        }
                    }
                }
            }

            writer.Flush();
            writer.Close();
            writer.Dispose();
        }

        private void WriteFileNodeInfoAsText(DirectoryRaid.FileNode node)
        {
            var writer = this._writer;
            var sb = new StringBuilder();
            sb.Append(node.NodeType);
            sb.Append('|');
            sb.Append(node.ID.ToString("x"));
            sb.Append('|');
            sb.Append(node.ParentNodeID.ToString("x"));
            sb.Append('|');
            sb.Append(node.Storage.ID.ToString("x"));
            sb.Append('|');
            sb.Append(node.Size.ToString("x"));
            sb.Append('|');
            sb.Append(node.CreationTime.ToString("x"));
            sb.Append('|');
            sb.Append(node.LastWriteTime.ToString("x"));
            sb.Append('|');
            sb.Append(node.Name);
            writer.WriteLine(sb.ToString());
        }

        //
    }
}
