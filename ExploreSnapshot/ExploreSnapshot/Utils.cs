using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExploreSnapshot
{
    public class Utils
    {
        public static string Hex2WStr(string hex)
        {
            var result = string.Empty;
            var n = hex.Length;
            for (int i = 0; i < n; i += 2)
            {
                var s = hex.Substring(i, 2);
                var num = uint.Parse(s, System.Globalization.NumberStyles.AllowHexSpecifier);
                result += (char)num;
            }
            return result;
        }

        public static string WStr2Hex(string s)
        {
            var result = string.Empty;
            foreach (var c in s)
            {
                result += string.Format("{0:X2}", (uint)c);
            }
            return result;
        }

    }
}
