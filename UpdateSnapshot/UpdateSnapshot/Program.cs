using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace UpdateSnapshot
{
    class Program
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        private static int _fileIdIncr = 1;


        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("UpdateSnapshot <SnapshotHeaderFile> <PartitionNumber> <PartitionDataPath>");
                return;
            }

            var snapshotHdrFile = args[0];
            var data = File.ReadAllText(snapshotHdrFile);
            var hdr = JsonConvert.DeserializeObject<DirectoryRaid.Header>(data);

            if (!hdr.Status.Equals("updating", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Error: invalid status.");
                return;
            }

            var partNumber = uint.Parse(args[1]);
            if (partNumber <= 0 || partNumber > hdr.NumberOfPartitions)
            {
                Console.WriteLine("Error: invalid partition number.");
                return;
            }

            var partitionRootDir = args[2];
            var snapshotID = hdr.ID;
            var tree = BuildDataDirTree(Path.Combine(partitionRootDir, "data"));

            var partitionHdr = new DirectoryRaid.PartitionHeader();
            partitionHdr.Label = tree.volumeLabel;
            partitionHdr.RelativePath = RemoveDriveRootDir(partitionRootDir);
            hdr.Partitions[partNumber - 1] = partitionHdr;

            data = JsonConvert.SerializeObject(hdr, Formatting.Indented);
            File.WriteAllText(snapshotHdrFile, data);

            var fi = new FileInfo(snapshotHdrFile);
            var outputFile = fi.Directory.FullName;
            outputFile = Path.Combine(outputFile, "list-" + partNumber.ToString() + ".txt");

            var writer = File.CreateText(outputFile);
            var storageHdr = string.Empty;
            storageHdr += "S|0|0|";
            storageHdr += "|";
            storageHdr += snapshotID;
            storageHdr += "|";
            storageHdr += partNumber.ToString();
            storageHdr += "|";
            storageHdr += tree.size.ToString();
            storageHdr += "|";
            storageHdr += tree.volumeLabel;
            storageHdr += "|";
            storageHdr += tree.relPath;
            writer.WriteLine(storageHdr);

            WriteDirList(writer, tree);

            writer.WriteLine(storageHdr);
            writer.Flush();
            writer.Close();
        }

        private static void WriteDirList(StreamWriter writer, RaidDir dir)
        {
            long parentID = 0;
            var parentDir = dir.parent;
            if (parentDir != null)
            {
                parentID = parentDir.id;
            }

            var s = string.Empty;
            s += "D|";
            s += dir.id.ToString();
            s += "|";
            s += parentID.ToString();
            s += "|";
            s += dir.size.ToString();
            s += "|";
            s += dir.creationTime.ToString("X");
            s += "|";
            s += dir.lastWriteTime.ToString("X");
            s += "|";
            s += dir.name;
            writer.WriteLine(s);

            var dirID = dir.id;
            var files = dir.files;
            foreach (var file in files)
            {
                if (file.IsFile)
                {
                    s = string.Empty;
                    s += "F|";
                    s += file.id.ToString();
                    s += "|";
                    s += dirID.ToString();
                    s += "|";
                    s += file.size.ToString();
                    s += "|";
                    s += file.creationTime.ToString("X");
                    s += "|";
                    s += file.lastWriteTime.ToString("X");
                    s += "|";
                    s += file.name;
                    writer.WriteLine(s);
                }
            }

            foreach (var subDir in files)
            {
                if (!subDir.IsFile)
                {
                    WriteDirList(writer, subDir as RaidDir);
                }
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

        private class RaidFile
        {
            public long id = 0;
            public string name = string.Empty;
            public long size = 0;
            public long creationTime = 0;
            public long lastWriteTime = 0;

            public virtual bool IsFile { get { return true; } }

            public override string ToString()
            {
                return this.name;
            }
        }

        private class RaidDir : RaidFile
        {
            public RaidDir parent = null;
            public List<RaidFile> files = new List<RaidFile>();
            public override bool IsFile { get { return false; } }
        }

        private class RaidDrive : RaidDir
        {
            public string volumeLabel = string.Empty;
            public string relPath = string.Empty;

            public override string ToString()
            {
                return this.volumeLabel;
            }
        }

        private static long DateTimeToTimestamp(DateTime t)
        {
            var dt = t - UnixEpoch;
            return (long)dt.TotalSeconds;
        }

        private static RaidDrive BuildDataDirTree(string path)
        {
            var label = ResolveVolumeLabel(path);
            if (null == label)
            {
                throw new Exception("Volume label is required.");
            }

            var root = new RaidDrive();
            root.id = _fileIdIncr++;
            root.volumeLabel = label;
            root.relPath = RemoveDriveRootDir(path);
            ScanDir(root, path);

            return root;
        }

        private static void ScanDir(RaidDir currentDir, string path)
        {
            Console.WriteLine(path);

            string[] fileList = null;
            try
            {
                fileList = Directory.GetFiles(path);
            }
            catch (Exception)
            {
                fileList = null;
            }
            if (fileList != null)
            {
                foreach (var filename in fileList)
                {
                    var file = new RaidFile();
                    file.id = _fileIdIncr++;

                    var info = new FileInfo(filename);
                    file.name = info.Name;
                    file.creationTime = DateTimeToTimestamp(info.CreationTime);
                    file.lastWriteTime = DateTimeToTimestamp(info.LastWriteTime);
                    file.size = info.Length;

                    currentDir.files.Add(file);
                    currentDir.size += file.size;
                }
            }

            try
            {
                fileList = Directory.GetDirectories(path);
            }
            catch (Exception)
            {
                fileList = null;
            }
            if (fileList != null)
            {
                foreach (var filename in fileList)
                {
                    var subDir = new RaidDir();
                    subDir.id = _fileIdIncr++;

                    var info = new DirectoryInfo(filename);
                    subDir.name = info.Name;
                    subDir.creationTime = DateTimeToTimestamp(info.CreationTime);
                    subDir.lastWriteTime = DateTimeToTimestamp(info.LastWriteTime);
                    ScanDir(subDir, filename);

                    subDir.parent = currentDir;
                    currentDir.files.Add(subDir);
                    currentDir.size += subDir.size;
                }
            }

            SortDir(currentDir);
        }

        private static int RFileCmp(RaidFile file1, RaidFile file2)
        {
            var d = file2.size - file1.size;
            if (d > 0)
            {
                return 1;
            }
            else if (d < 0)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }

        private static void SortDir(RaidDir dir)
        {
            var files = dir.files;
            var ar1 = files.ToArray();
            Array.Sort(ar1, RFileCmp);
            files.Clear();
            files.AddRange(ar1);
        }

        //
    }
}
