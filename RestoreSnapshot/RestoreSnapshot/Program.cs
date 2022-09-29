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
                Console.WriteLine("    Label: show all storage headers");
                return;
            }

            var mode = args[0].ToLower();
            var hdrFileName = args[1];

            var hdrData = File.ReadAllText(hdrFileName);
            _Header = JsonConvert.DeserializeObject<DirectoryRaid.Header>(hdrData);

            var fi = new FileInfo(hdrFileName);
            var cwd = fi.Directory.FullName;

            _DB = new RaidDB();
            _DB.Load(Path.Combine(cwd, "blocks.db"));

            if (mode.Equals("build", StringComparison.Ordinal))
            {

            }

        }


    }
}
