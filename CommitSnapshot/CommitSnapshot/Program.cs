using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CommitSnapshot
{
    class Program
    {
        private static DirectoryRaid.RaidHeader Header = null;

        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("CommitSnapshot <SnapshotHeaderFile>");
                return;
            }

            var hdrFileName = args[0];
            var hdrData = File.ReadAllText(hdrFileName);
            Header = JsonConvert.DeserializeObject<DirectoryRaid.RaidHeader>(hdrData);
            if (!Header.Status.Equals("updating", StringComparison.OrdinalIgnoreCase))
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

            var dicFileTrees = new Dictionary<int, string[]>();
            var id = Header.ID;
            var n = Header.NumberOfPartitions;
            for (int i = 1; i <= n; ++i)
            {
                var fileName = Path.Combine(cwd, "list-" + i.ToString() + ".txt");
                var rows = File.ReadAllLines(fileName);
                dicFileTrees[i] = rows;
            }

            Header.MaximumPartSize = 0;
            foreach (var kv in dicFileTrees)
            {
                var partTotalSize = CalculateTotalSize(kv.Value);
                Header.MaximumPartSize = (long)Math.Max(Header.MaximumPartSize, partTotalSize);
            }

            foreach (var kv in dicFileTrees)
            {
                var fileName = Path.Combine(cwd, "blck-" + kv.Key + ".txt");
                var rows = SplitParts(kv.Key, kv.Value);
                File.WriteAllLines(fileName, rows);
            }

            Header.Status = "Committed";
            hdrData = JsonConvert.SerializeObject(Header, Formatting.Indented);
            File.WriteAllText(hdrFileName, hdrData);
        }

        private static long CalculateTotalSize(string[] rows)
        {
            long result = 0;
            foreach (var row in rows)
            {
                var cells = row.Split('|');
                if (cells.Length < 7)
                {
                    continue;
                }

                var recType = cells[0].ToUpper();
                if (!recType.Equals("F", StringComparison.Ordinal))
                {
                    continue;
                }

                result += long.Parse(cells[3]);
            }
            return result;
        }

        private static string[] SplitParts(int partNum, string[] rows)
        {
            var result = new List<string>();
            var storageRow = string.Empty;
            foreach (var row in rows)
            {
                var cells = row.Split('|');
                if (cells.Length < 2)
                {
                    continue;
                }

                var recType = cells[0].ToUpper();
                if (recType.Equals("S", StringComparison.Ordinal))
                {
                    storageRow = row;
                }
                else if (recType.Equals("D", StringComparison.Ordinal))
                {
                    result.Add(row);
                }
            }
            result.Insert(0, storageRow);

            foreach (var row in rows)
            {
                var cells = row.Split('|');
                if (cells.Length < 2)
                {
                    continue;
                }

                var recType = cells[0].ToUpper();
                if (recType.Equals("F", StringComparison.Ordinal))
                {
                    result.Add(row);
                }
            }

            var blockID = (long)1;
            var blockSubID = (long)0;
            var blockSize = Header.BlockSize;
            var prevRemain = blockSize;
            foreach (var row in rows)
            {
                var cells = row.Split('|');
                if (cells.Length < 7)
                {
                    continue;
                }

                var recType = cells[0].ToUpper();
                if (!recType.Equals("F", StringComparison.Ordinal))
                {
                    continue;
                }

                var fileID = cells[1];
                var size = long.Parse(cells[3]);
                result.Add(row);

                var offset = (long)0;
                while (offset < size)
                {
                    var remain = size - offset;
                    var actualBlockSize = (long)Math.Min(prevRemain, remain);
                    actualBlockSize = Math.Min(actualBlockSize, blockSize);

                    var sb = new StringBuilder();
                    sb.Append("B|");
                    sb.Append(blockID.ToString());
                    sb.Append("|");
                    sb.Append(blockSubID.ToString());
                    sb.Append("|");
                    sb.Append(fileID);
                    sb.Append("|");
                    sb.Append(offset.ToString("X"));
                    sb.Append("|");
                    if (actualBlockSize < blockSize)
                    {
                        sb.Append(actualBlockSize.ToString("X"));
                    }
                    sb.Append("|");
                    result.Add(sb.ToString());

                    ++blockSubID;
                    if (actualBlockSize >= prevRemain)
                    {
                        ++blockID;
                        blockSubID = 0;
                        prevRemain = blockSize;
                    }
                    else
                    {
                        prevRemain -= actualBlockSize;
                    }
                    offset += actualBlockSize;
                }
                //
            }

            return result.ToArray();
        }

        //
    }
}
