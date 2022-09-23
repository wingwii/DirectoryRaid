using System;
using System.Collections.Generic;
using System.Text;

namespace DirectoryRaid
{
    public class RaidHeader
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

        public override string ToString()
        {
            return this.Name;
        }
    }
}