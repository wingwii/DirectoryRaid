using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.JSInterop;
using Newtonsoft.Json;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ExploreSnapshot.API
{
    [Route("api/[controller]")]
    [ApiController]
    public class LocalFileBrowserController : ControllerBase
    {
        // GET: api/<LocalFileBrowserController>
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/<LocalFileBrowserController>/5
        [HttpGet("{id}")]
        public string Get(string id)
        {
            string rootPath = null;
            string[] subDirList = null;
            string[] fileList = null;

            try { rootPath = Utils.Hex2WStr(id); }
            catch (Exception) { }
            try { subDirList = Directory.GetDirectories(rootPath); }
            catch (Exception) { }
            try { fileList = Directory.GetFiles(rootPath); }
            catch (Exception) { }

            var dic1 = new Dictionary<string, object>();
            dic1["subdirs"] = PreprocessFileList(subDirList);
            dic1["files"] = PreprocessFileList(fileList);

            var result = JsonConvert.SerializeObject(dic1);
            return result;
        }

        // POST api/<LocalFileBrowserController>
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/<LocalFileBrowserController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<LocalFileBrowserController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        private static object PreprocessFileList(string[] fileList)
        {
            if (null == fileList)
            {
                return null;
            }

            var result = new List<string[]>();
            foreach (var filename in fileList)
            {
                var fi = new FileInfo(filename);
                result.Add(new string[] {
                    fi.Name,
                    Utils.WStr2Hex(filename)
                });
            }
            return result.ToArray();
        }
    }
}

