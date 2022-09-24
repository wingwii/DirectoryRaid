using Newtonsoft.Json;
using System;
using System.IO;

namespace CreateSnapshot
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine("CreateSnapshot <NumberOfPartitions> <BlockSize> <Name> <RaidPath>");
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

            long blockSize = 0;
            var s = args[1].ToLower();
            var name = args[2];
            var raidPath = args[3];

            if (s.EndsWith("k", StringComparison.Ordinal))
            {
                blockSize = long.Parse(s.Substring(0, s.Length - 1));
                blockSize *= 1024;
            }
            else if (s.EndsWith("m", StringComparison.Ordinal))
            {
                blockSize = long.Parse(s.Substring(0, s.Length - 1));
                blockSize *= 0x100000;
            }
            else
            {
                blockSize = long.Parse(s);
            }

            var label = ResolveVolumeLabel(raidPath);
            if (string.IsNullOrEmpty(label))
            {
                Console.WriteLine("Error: volume label is required.");
                return;
            }

            var relRaidPath = RemoveDriveRootDir(raidPath);

            var hdr = new DirectoryRaid.RaidHeader();
            hdr.CreationTime = now.ToString("yyyy-MM-dd HH:mm:ss");
            hdr.ID = snapshotID;
            hdr.Name = name;
            hdr.NumberOfPartitions = numberOfParts;
            hdr.BlockSize = blockSize;
            hdr.VolumeLabel = label;
            hdr.RelativePath = relRaidPath;
            hdr.Status = "Updating";

            var outputPath = Path.Combine(raidPath, snapshotID);
            CreateDirectorySafely(outputPath);

            var metaPath = Path.Combine(outputPath, "meta");
            CreateDirectorySafely(metaPath);

            var data = JsonConvert.SerializeObject(hdr, Formatting.Indented);
            var hdrFileName = Path.Combine(metaPath, "header.json");
            File.WriteAllText(hdrFileName, data);

            Console.WriteLine("Snapshot \"" + snapshotID + "\" was created.");
        }

        private static void CreateDirectorySafely(string path)
        {
            try { Directory.CreateDirectory(path); }
            catch (Exception) { }
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
