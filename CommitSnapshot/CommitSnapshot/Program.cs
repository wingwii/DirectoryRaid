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
            var s = File.ReadAllText(hdrFileName);
            Header = JsonConvert.DeserializeObject<DirectoryRaid.RaidHeader>(s);
            if (!Header.Status.Equals("updating", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Invalid status");
                return;
            }

            var fi = new FileInfo(hdrFileName);
            var cwd = fi.Directory.FullName;

            var dicFileTrees = new Dictionary<int, string[]>();
            var id = Header.ID;
            var n = Header.NumberOfPartitions;
            for (int i = 0; i < n; ++i)
            {
                var fileName = Path.Combine(cwd, id + ".l" + i.ToString());
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
                var fileName = Path.Combine(cwd, id + ".b" + kv.Key);
                var rows = SplitParts(kv.Value);
                File.WriteAllLines(fileName, rows);
            }
            //            
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

        private static string[] SplitParts(string[] rows)
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

            var idIncr = (long)1;
            var prevRemain = (long)0;
            var blockSize = Header.BlockSize;
            var totalRemain = Header.MaximumPartSize;
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
                result.Add(row);

                var size = long.Parse(cells[3]);
                var offset = (long)0;
                var blockIdx = offset;
                while (offset < size)
                {
                    var remain = size - offset;
                    var actualBlockSize = remain;
                    if (prevRemain > 0)
                    {
                        if (actualBlockSize > prevRemain)
                        {
                            actualBlockSize = prevRemain;
                        }
                        prevRemain -= actualBlockSize;
                    }
                    else
                    {
                        if (actualBlockSize > blockSize)
                        {
                            actualBlockSize = blockSize;
                        }
                    }
                    if (remain < blockSize)
                    {
                        prevRemain = blockSize - remain;
                    }

                    var sb = new StringBuilder();
                    sb.Append(" B|");
                    sb.Append(idIncr.ToString());
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

                    ++idIncr;
                    ++blockIdx;
                    offset += actualBlockSize;
                }
                //
            }

            return result.ToArray();
        }

        //
    }
}
