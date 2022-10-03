using System;
using System.Collections.Generic;
using System.Text;

namespace DirectoryRaid
{
    public class PartitionHeader
    {
        public string Label { get; set; } = string.Empty;
        public string RelativePath { get; set; } = string.Empty;
    }

    public class Header
    {
        public string ID { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public uint NumberOfPartitions { get; set; } = 0;
        public long BlockSize { get; set; } = 0;
        public string CreationTime { get; set; } = string.Empty;
        public string VolumeLabel { get; set; } = string.Empty;
        public string RelativePath { get; set; } = string.Empty;
        public long MaximumPartSize { get; set; } = 0;
        public string Status { get; set; } = string.Empty;
        public PartitionHeader[] Partitions { get; set; } = null;

        public override string ToString()
        {
            return this.Name;
        }
    }

    public class Node
    {
        public long ID { get; set; } = 0;
        public string NodeType { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public long Size { get; set; } = 0;

        public override string ToString()
        {
            return this.Name;
        }
    }

    public class StorageNode : Node
    {
        public string SnapshotID { get; set; } = null;
        public int StorageNumber { get; set; } = 0;
        public string RelativePath { get; set; } = string.Empty;
        public Node RootDirectory { get; set; } = null;
        public Node[] AllFileNodes { get; set; } = null;
    }

    public class FileNode : Node
    {
        public StorageNode Storage { get; set; } = null;
        public Node ParentNode { get; set; } = null;
        public long CreationTime { get; set; } = 0;
        public long LastWriteTime { get; set; } = 0;
        public virtual bool IsFile { get { return true; } }
    }

    public class DirectoryNode : FileNode
    {
        public List<FileNode> Files = new List<FileNode>();
        public override bool IsFile { get { return false; } }
    }

    public class FilePart
    {
        public long ID { get; set; } = 0;
        public int PartNumber { get; set; } = 0;
        public FileNode DataFile { get; set; } = null;
        public long Offset { get; set; } = 0;
        public long Size { get; set; } = 0;
    }

    public class FilePartsGroup
    {
        public long ID { get; set; } = 0;
        public StorageNode Storage { get; set; } = null;
        public FilePart[] Items { get; set; } = null;
        public long OffsetToDataHash { get; set; } = 0;
        public byte[] DataHash { get; set; } = null;
    }

    public class RaidDataBlock
    {
        public long ID { get; set; } = 0;
        public long BlockNumber { get; set; } = 0;
        public long Size { get; set; } = 0;
        public FilePartsGroup[] Items { get; set; } = null;
    }

    //
}