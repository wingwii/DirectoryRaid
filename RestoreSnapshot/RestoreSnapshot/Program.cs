using Newtonsoft.Json;
using System;
using System.IO;

namespace RestoreSnapshot
{
    class Program
    {
        private static DirectoryRaid.Header _Header = null;
        private static RaidDB _DB = null;

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("restore <mode> <DbPath>");
                Console.WriteLine("  Modes:");
                Console.WriteLine("    Build");
                return;
            }

            var mode = args[0].ToLower();
            var hdrFileName = args[1];

            var hdrData = File.ReadAllText(hdrFileName);
            _Header = JsonConvert.DeserializeObject<DirectoryRaid.Header>(hdrData);

            var fi = new FileInfo(hdrFileName);
            var cwd = fi.Directory.FullName;

            _DB = new RaidDB();
            _DB.RaidHeader = _Header;
            _DB.FileName = Path.Combine(cwd, "blocks.db");
            _DB.Load();

            if (mode.Equals("build", StringComparison.Ordinal))
            {
                var builder = new Builder(_DB);
                builder.Build(_Header.NumberOfPartitions + 1);
            }

#if DEBUG
            //Console.ReadKey();
#endif
        }

    }
}
