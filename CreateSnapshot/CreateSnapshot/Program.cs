using Newtonsoft.Json;
using System;
using System.IO;

namespace CreateSnapshot
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("CreateSnapshot <NumberOfPartitions> <Name> <RaidPath>");
                return;
            }

            var now = DateTime.Now;
            var snapshotID = GenerateSnapshotID(now);

            var numberOfParts = uint.Parse(args[0]);
            if (numberOfParts > 99)
            {
                Console.WriteLine("Error: supports maximum 99 partitions.");
                return;
            }

            var raidPath = args[2];
            var label = ResolveVolumeLabel(raidPath);
            if (string.IsNullOrEmpty(label))
            {
                Console.WriteLine("Error: volume label is required.");
                return;
            }

            var relRaidPath = RemoveDriveRootDir(raidPath);

            var hdr = new RaidHeaderS1();
            hdr.CreationTime = now.ToString("yyyy-MM-dd HH:mm:ss");
            hdr.ID = snapshotID;
            hdr.Name = args[1];
            hdr.NumberOfPartitions = numberOfParts;
            hdr.VolumeLabel = label;
            hdr.RelativePath = relRaidPath;

            var data = JsonConvert.SerializeObject(hdr, Formatting.Indented);
            var hdrFileName = Path.Combine(raidPath, snapshotID + ".hdr");
            File.WriteAllText(hdrFileName, data);

            try
            {
                Directory.CreateDirectory(Path.Combine(raidPath, snapshotID));
            }
            catch (Exception) { }

            Console.WriteLine("Snapshot \"" + snapshotID + "\" was created.");
        }

        private class RaidHeaderS1
        {
            public string ID { get; set; } = string.Empty;
            public string Name { get; set; } = string.Empty;
            public string CreationTime { get; set; } = string.Empty;
            public uint NumberOfPartitions { get; set; } = 0;
            public string VolumeLabel { get; set; } = string.Empty;
            public string RelativePath { get; set; } = string.Empty;


            public override string ToString()
            {
                return this.Name;
            }
        }

        private static string RemoveDriveRootDir(string path)
        {
            var relPath = path;
            var pos = relPath.IndexOf(":\\", StringComparison.Ordinal);
            if (pos > 0)
            {
                relPath = relPath.Substring(pos + 2);
            }
            return relPath;
        }

        private static string ResolveVolumeLabel(string path)
        {
            var drives = DriveInfo.GetDrives();
            foreach (var drive in drives)
            {
                var rootDir = drive.RootDirectory.FullName;
                if (path.StartsWith(rootDir, StringComparison.OrdinalIgnoreCase))
                {
                    return drive.VolumeLabel;
                }
            }
            return null;
        }

        private static string GenerateSnapshotID(DateTime t)
        {
            uint num = 0;
            num += (uint)(t.Year);
            num *= 12;
            num += (uint)(t.Month - 1);
            num *= 31;
            num += (uint)(t.Day - 1);
            num *= 24;
            num += (uint)(t.Hour);
            num *= 60;
            num += (uint)(t.Minute);
            num *= 60;
            num += (uint)(t.Second);
            return string.Format("{0:x8}", num);
        }

    }
}
