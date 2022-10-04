using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace ExploreSnapshot.Pages
{
    public class ExplorerModel : PageModel
    {
        public void OnGet()
        {
            var filename = Utils.Hex2WStr(this.Request.Query["path"]);
            var db = new RaidDB();
            db.Load(filename);
        }
    }
}
