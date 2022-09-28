using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CommitSnapshot
{
    class Program
    {
        private static DirectoryRaid.Header _Header = null;
        private static Dictionary<int, DirectoryRaid.StorageNode> _DicStorages = new Dictionary<int, DirectoryRaid.StorageNode>();
        private static RaidStorage[] _RaidArray = null;
        private static List<DirectoryRaid.RaidDataBlock> _Results = new List<DirectoryRaid.RaidDataBlock>();


        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("CommitSnapshot <SnapshotHeaderFile>");
                return;
            }

            var hdrFileName = args[0];
            var hdrData = File.ReadAllText(hdrFileName);
            _Header = JsonConvert.DeserializeObject<DirectoryRaid.Header>(hdrData);
            if (!_Header.Status.Equals("updating", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Invalid status");
#if DEBUG
                //
#else
                return;
#endif
            }

            var fi = new FileInfo(hdrFileName);
            var cwd = fi.Directory.FullName;

            _Header.MaximumPartSize = 0;
            var metaParser = new DirectoryRaid.MetaStorageParser();
            var id = _Header.ID;
            var n = _Header.NumberOfPartitions;
            for (int i = 1; i <= n; ++i)
            {
                var fileName = Path.Combine(cwd, "list-" + i.ToString() + ".txt");
                var rows = File.ReadAllLines(fileName);
                var storage = metaParser.Parse(rows);
                _DicStorages[i] = storage;

                _Header.MaximumPartSize = (long)Math.Max(_Header.MaximumPartSize, storage.Size);

                var nodes = storage.AllFileNodes;
                Array.Sort(nodes, CmpFileNode);
            }

            PrepareRaidBlocks(metaParser.NewBaseID);

            var export1 = new TxtFileExport(_RaidArray, _Results);
            export1.SaveAs(Path.Combine(cwd, "blocks.txt"));

            var export2 = new BinFileExport(_RaidArray, _Results);
            export2.SaveAs(Path.Combine(cwd, "blocks.db"));

            _Header.Status = "Committed";
            hdrData = JsonConvert.SerializeObject(_Header, Formatting.Indented);
            File.WriteAllText(hdrFileName, hdrData);
        }

        private static int CmpFileNode(DirectoryRaid.Node node1, DirectoryRaid.Node node2)
        {
            var size1 = node1.Size;
            var size2 = node2.Size;
            if (size1 < size2)
            {
                return 1;
            }
            else if (size1 > size2)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }

        private static void PrepareRaidBlocks(long baseObjID)
        {
            var currentObjID = baseObjID + 1;

            var blockSize = _Header.BlockSize;
            _RaidArray = new RaidStorage[_DicStorages.Count];
            foreach (var kv in _DicStorages)
            {
                var idx = kv.Key - 1;
                var rs = new RaidStorage(blockSize, kv.Value);
                _RaidArray[idx] = rs;
                rs.Start();
            }

            long currentBlockID = 1;
            while (true)
            {
                var blck = new DirectoryRaid.RaidDataBlock();
                blck.ID = currentObjID;
                blck.BlockNumber = currentBlockID;
                blck.Size = blockSize;
                blck.Items = new DirectoryRaid.FilePartsGroup[_RaidArray.Length];

                ++currentObjID;

                int raidArrIdx = 0;
                int nonNullCount = 0;
                foreach (var rs in _RaidArray)
                {
                    var parts = rs.Next();

                    var group = new DirectoryRaid.FilePartsGroup();
                    blck.Items[raidArrIdx] = group;

                    group.ID = currentObjID;
                    group.Storage = rs.Storage;

                    ++currentObjID;

                    if (parts != null)
                    {
                        var partCount = parts.Length;
                        group.Items = new DirectoryRaid.FilePart[partCount];
                        for (int partIdx = 0; partIdx < partCount; ++partIdx)
                        {
                            var part = parts[partIdx];
                            var datPart = new DirectoryRaid.FilePart();
                            group.Items[partIdx] = datPart;

                            datPart.ID = currentObjID;
                            datPart.PartNumber = part.PartNumber;
                            datPart.DataFile = part.DataFile;
                            datPart.Offset = part.Offset;
                            datPart.Size = part.Size;

                            ++currentObjID;
                        }
                        ++nonNullCount;
                    }

                    ++raidArrIdx;
                }
                if (0 == nonNullCount)
                {
                    break;
                }

                _Results.Add(blck);
                ++currentBlockID;
            }
            //
        }

        //
    }
}
