using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace RestoreSnapshot
{
    class Builder
    {
        private DirectoryRaid.RaidDB _db = null;
        private int _workerCount = 0;
        private uint _dstStorageNumber = 0;
        private int _dstStorageIdx = -1;
        private Dictionary<int, int> _dicStorageNum2Idx = new Dictionary<int, int>();
        private DirectoryRaid.RaidDB.StorageNode _dstStorage = null;
        private string _dstStoragePath = string.Empty;
        private string[] _arActualStorageRootPaths = null;
        private bool[] _arActualStorageChecking = null;
        private int _builderStatusRecordLen = 0;
        private BuilderStatusRecord[] _arBuilderStatus = null;
        private string _builderStatusFileName = null;
        private int[] _arWorkerReaderBlockIdx = null;
        private byte[][] _arWorkerBuf = null;
        private int _workerReport = 0;


        public Builder(DirectoryRaid.RaidDB db)
        {
            this._db = db;
        }

        public bool IsRestorationMode = false;

        public bool Build(uint storageNumber)
        {
            this._dstStorageNumber = storageNumber;
            this._dstStorageIdx = this.FindStorageIndex(storageNumber);
            if (this._dstStorageIdx < 0)
            {
                return false;
            }
            this._dstStorage = this._db.Storages[this._dstStorageIdx];

            this.PrepareActualStorageRootPaths();

            this._dstStoragePath = this._arActualStorageRootPaths[this._dstStorageIdx];
            if (string.IsNullOrEmpty(this._dstStoragePath))
            {
                return false;
            }
            if (!this.IsRestorationMode)
            {
                if (!this._arActualStorageChecking[this._dstStorageIdx])
                {
                    return false;
                }
            }

            this._builderStatusRecordLen = 18 + (48 * this._db.Storages.Length);

            this.PrepareBuilderStatusFileName();
            this.PrepareWorkerThreads();

            this.InitBuilderStatus();
            this.LoadBuilderStatusFile();

            if (this._workerCount > 1)
            {
                int nFullBlock = 0;
                var dataBlocks = this._db.DataBlocks;
                var n = dataBlocks.Length;
                for (int i = 0; i < n; ++i)
                {
                    Console.Title = "Building " + i.ToString() + " of " + n.ToString();

                    if (this.IsFullBlock(i))
                    {
                        ++nFullBlock;
                    }
                    else
                    {
                        // Mark as "dirty"
                        var saveOK = this.SaveBuilderStatus(i, 1);
                        if (saveOK)
                        {
                            this.WakeAllReaders(i);
                            this.WaitForAllReaders();
                            this.ComputeCurrentParityBlock();
                            this.UpdateBuilderStatus(i);

                            int status = 2; // partially completed
                            var isFullBlck = this.IsFullBlock(i);
                            if (isFullBlck)
                            {
                                ++nFullBlock;
                                status = 3; // fully backed up
                            }

                            saveOK = this.SaveRaidBlock(dataBlocks[i]);
                            if (saveOK)
                            {
                                this.SaveBuilderStatus(i, status);
                            }
                        }
                        //
                    }
                }

                if (nFullBlock == n)
                {
                    if (!this.IsRestorationMode)
                    {
                        this._db.RaidHeader.Status = "Backup";
                        this.SaveHashesToDB();
                    }
                }
            }

            return true;
        }

        private void SaveHashesToDB()
        {
            var fs = File.OpenWrite(this._db.FileName);
            var storageCount = this._db.Storages.Length;
            var dataBlocks = this._db.DataBlocks;
            var dataBlockCount = dataBlocks.Length;
            for (int i = 0; i < dataBlockCount; ++i)
            {
                var block = dataBlocks[i];
                var record = this._arBuilderStatus[i];
                for (int j = 0; j < storageCount; ++j)
                {
                    var filePartGrp = block.DGroups[j];
                    var hashStr = record.arHashStr[j];

                    byte[] hash = null;
                    if (!string.IsNullOrEmpty(hashStr))
                    {
                        hashStr = hashStr.Trim();
                        if (!string.IsNullOrEmpty(hashStr))
                        {
                            if (!hashStr.Equals("*", StringComparison.Ordinal))
                            {
                                hash = Convert.FromBase64String(hashStr);
                            }
                        }
                    }

                    fs.Position = filePartGrp.OffsetToDataHash;
                    if (null == hash)
                    {
                        fs.WriteByte(0);
                        fs.Position += 32;
                    }
                    else
                    {
                        fs.WriteByte(1);
                        fs.Write(hash, 0, hash.Length);
                    }
                }
            }
            fs.Flush();
            fs.Close();
            fs.Dispose();
        }

        private bool IsFullBlock(int idx)
        {
            var bs = this._arBuilderStatus[idx];
            if (null == bs)
            {
                return false;
            }
            foreach (var hashStr in bs.arHashStr)
            {
                if (string.IsNullOrEmpty(hashStr))
                {
                    return false;
                }
                if (hashStr.Equals("*", StringComparison.Ordinal))
                {
                    return false;
                }
            }
            return true;
        }

        private class BuilderStatusRecord
        {
            public int status = 0;
            public string[] arHashStr = null;
        }

        private static byte[] CheckSumStorageBlock(byte[] buf)
        {
            byte[] result = null;
            using (var hashFunc = SHA256.Create())
            {
                result = hashFunc.ComputeHash(buf);
            }
            return result;
        }

        private void InitBuilderStatus()
        {
            var storageCount = this._db.Storages.Length;
            var n = this._db.DataBlocks.Length;
            this._arBuilderStatus = new BuilderStatusRecord[n];
            for (int i = 0; i < n; ++i)
            {
                var bs = new BuilderStatusRecord();
                this._arBuilderStatus[i] = bs;

                var arHashStr = new string[storageCount];
                bs.arHashStr = arHashStr;
                bs.status = 0;

                for (int j = 0; j < storageCount; ++j)
                {
                    arHashStr[j] = null;
                }
            }
        }

        private void UpdateBuilderStatus(int idx)
        {
            var bs = this._arBuilderStatus[idx];
            var arHashStr = bs.arHashStr;

            var storageCount = this._db.Storages.Length;
            for (int i = 0; i < storageCount; ++i)
            {
                string hashStr = arHashStr[i];
                if (null == hashStr || i == this._dstStorageIdx)
                {
                    var storageAlive = this._arActualStorageChecking[i];
                    if (storageAlive)
                    {
                        var hash = CheckSumStorageBlock(this._arWorkerBuf[i]);
                        hashStr = Convert.ToBase64String(hash);
                    }
                }
                bs.arHashStr[i] = hashStr;
            }
        }

        private void LoadBuilderStatusFile()
        {
            if (!File.Exists(this._builderStatusFileName))
            {
                return;
            }

            int recIdx = 0;
            var storageCount = this._db.Storages.Length;
            var recLen = this._builderStatusRecordLen;
            var buf = new byte[recLen];
            var fs = File.OpenRead(this._builderStatusFileName);
            while (fs.Position < fs.Length)
            {
                var nRead = fs.Read(buf, 0, recLen);
                if (nRead < recLen)
                {
                    break;
                }

                var s = Encoding.ASCII.GetString(buf);
                var s2 = s.Substring(0, 8);
                recIdx = int.Parse(s2, System.Globalization.NumberStyles.HexNumber);

                var bs = this._arBuilderStatus[recIdx];
                var arHashStr = bs.arHashStr;

                s2 = s.Substring(15, 1);
                var status = int.Parse(s2);
                bs.status = status;

                s2 = s.Substring(16);
                for (int j = 0; j < storageCount; ++j)
                {
                    var hashStr = s2.Substring(4, 44).Trim();
                    s2 = s2.Substring(48);

                    if (status < 2)
                    {
                        hashStr = null;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(hashStr))
                        {
                            hashStr = null;
                        }
                        else
                        {
                            if (hashStr.Equals("*", StringComparison.Ordinal))
                            {
                                hashStr = null;
                            }
                        }
                    }
                    arHashStr[j] = hashStr;
                }
            }
            fs.Close();
            fs.Dispose();
        }

        private bool SaveBuilderStatus(int idx, int newStatus)
        {
            var storageCount = this._arWorkerBuf.Length;
            long recordLen = this._builderStatusRecordLen;

            var sb = new StringBuilder();
            sb.Append(string.Format("{0:X8}", idx));
            sb.Append('|');
            sb.Append(storageCount.ToString().PadRight(2, ' '));
            sb.Append('|');
            sb.Append(this._dstStorageIdx.ToString().PadRight(2, ' '));

            var bs = this._arBuilderStatus[idx];
            var status = bs.status;
            if (newStatus >= 0)
            {
                status = newStatus;
            }

            sb.Append('|');
            sb.Append(status.ToString());

            for (int i = 0; i < storageCount; ++i)
            {
                sb.Append("\r\n  ");
                var hashStr = bs.arHashStr[i];
                if (null == hashStr)
                {
                    hashStr = "*";
                }
                sb.Append(hashStr.PadRight(44, ' '));
            }
            sb.Append("\r\n");

            var s = sb.ToString();
            var buf2 = Encoding.ASCII.GetBytes(s);

            var result = false;
            FileStream fs = null;
            try
            {
                fs = File.Open(this._builderStatusFileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
                fs.Position = idx * recordLen;
                fs.Write(buf2, 0, buf2.Length);
                fs.Flush();
                result = true;
            }
            catch (Exception) { }
            if (fs != null)
            {
                try { fs.Close(); }
                catch (Exception) { }
                try { fs.Dispose(); }
                catch (Exception) { }
            }
            return result;
        }

        private static void PrepareDir(string fileName)
        {
            var stack = new Stack<string>();
            var fi = new FileInfo(fileName);
            var path = fi.Directory.FullName;
            while (true)
            {
                var di = new DirectoryInfo(path);
                stack.Push(path);
                var parent = di.Parent;
                if (null == parent)
                {
                    break;
                }
                path = parent.FullName;
            }
            while (stack.Count > 0)
            {
                path = stack.Pop();
                try { Directory.CreateDirectory(path); }
                catch (Exception) { }
            }
        }

        private bool SaveRaidBlock(DirectoryRaid.RaidDB.DataBlock block)
        {
            var result = true;
            var buf = this._arWorkerBuf[this._dstStorageIdx];
            var blockSize = buf.Length;
            foreach (var grp in block.DGroups)
            {
                if (this._dstStorage != grp.Storage)
                {
                    continue;
                }

                long offset = 0;
                foreach (var part in grp.Parts)
                {
                    var partSize = part.Size;
                    var fileName = this.PrepareActualFilePath(this._dstStorageIdx, part.DataFile);

                    var s = "[" + part.Offset.ToString("X") + "] " + fileName;
                    Console.WriteLine(s);

                    FileStream fs = null;
                    try
                    {
                        PrepareDir(fileName);
                        fs = File.OpenWrite(fileName);
                        fs.Position = part.Offset;
                        fs.Write(buf, (int)offset, (int)partSize);
                        fs.Flush();
                    }
                    catch (Exception)
                    {
                        result = false;
                    }
                    try { fs.Close(); }
                    catch (Exception) { }
                    try { fs.Dispose(); }
                    catch (Exception) { }

                    offset += partSize;
                }
                break;
            }
            return result;
        }

        private void ComputeCurrentParityBlock()
        {
            var buf = this._arWorkerBuf[this._dstStorageIdx];
            var blockSize = buf.Length;
            foreach (var buf2 in this._arWorkerBuf)
            {
                if (null == buf2 || buf2 == buf)
                {
                    continue;
                }
                for (int i = 0; i < blockSize; ++i)
                {
                    buf[i] = (byte)((uint)buf[i] ^ (uint)buf2[i]);
                }
            }
        }

        private void PrepareBuilderStatusFileName()
        {
            var fileName = this._dstStoragePath;
            if (this.IsRestorationMode)
            {
                var di = new DirectoryInfo(fileName);
                fileName = di.Parent.FullName;
            }
            fileName = Path.Combine(fileName, "meta");
            try { Directory.CreateDirectory(fileName); }
            catch (Exception) { }
            this._builderStatusFileName = Path.Combine(fileName, "builder.sav");
        }

        private void WakeAllReaders(int blockIdx)
        {
            this._workerReport = this._workerCount;
            var n = this._arWorkerReaderBlockIdx.Length;
            for (int i = 0; i < n; ++i)
            {
                var buf = this._arWorkerBuf[i];
                ZeroBuf(buf, 0, buf.Length);

                var workerActivated = this._arActualStorageChecking[i];
                if (workerActivated)
                {
                    var workerRejected = false;
                    var bs = this._arBuilderStatus[blockIdx];
                    var status = bs.status;
                    if (i != this._dstStorageIdx)
                    {
                        if (status >= 2)
                        {
                            if (!string.IsNullOrEmpty(bs.arHashStr[i]))
                            {
                                workerRejected = true;
                            }
                        }
                    }
                    else
                    {
                        if (status < 2)
                        {
                            workerRejected = true;
                        }
                    }
                    if (workerRejected)
                    {
                        --this._workerReport;
                        continue;
                    }
                }

                this._arWorkerReaderBlockIdx[i] = blockIdx;
            }
        }

        private void WaitForAllReaders()
        {
            while (this._workerReport > 0)
            {
                Thread.Sleep(10);
            }
        }

        private static void ZeroBuf(byte[] buf, int offset, int size)
        {
            for (int i = 0; i < size; ++i)
            {
                buf[offset + i] = 0;
            }
        }

        private string PrepareActualFilePath(int storageIdx, DirectoryRaid.RaidDB.FileNode file)
        {
            var storage = file.Storage as DirectoryRaid.RaidDB.StorageNode;
            var rootDir = storage.RootDirectory as DirectoryRaid.RaidDB.FileNode;

            var stack = new Stack<string>();
            var node = file;
            while (node != null)
            {
                if (node == rootDir)
                {
                    break;
                }

                stack.Push(node.Name);
                node = node.Parent as DirectoryRaid.RaidDB.FileNode;
            }

            var path = this._arActualStorageRootPaths[storageIdx];
            while (stack.Count > 0)
            {
                var part = stack.Pop();
                path = Path.Combine(path, part);
            }

            return path;
        }

        private static DirectoryRaid.RaidDB.FilePartsGroup FindAssocFilePartGroup(DirectoryRaid.RaidDB.DataBlock dataBlock, int storageNumber)
        {
            foreach (var grp in dataBlock.DGroups)
            {
                if (storageNumber == grp.Storage.StorageNumber)
                {
                    return grp;
                }
            }
            return null;
        }

        private bool RunSrcReader(int storageIdx, int blockIdx)
        {
            var storage = this._db.Storages[storageIdx];
            var dataBlock = this._db.DataBlocks[blockIdx];
            var filePartGrp = FindAssocFilePartGroup(dataBlock, storage.StorageNumber);
            if (null == filePartGrp)
            {
                return false;
            }

            long offset = 0;
            var buf = this._arWorkerBuf[storageIdx];

            var fileParts = filePartGrp.Parts;
            foreach (var part in fileParts)
            {
                var fileName = this.PrepareActualFilePath(storageIdx, part.DataFile);
                var partSize = part.Size;

                FileStream fs = null;
                try
                {
                    fs = File.OpenRead(fileName);
                    fs.Position = part.Offset;

                    long actualSize = 0;
                    while (actualSize < partSize)
                    {
                        var nRead = fs.Read(buf, (int)(offset + actualSize), (int)(partSize - actualSize));
                        if (nRead <= 0)
                        {
                            break;
                        }
                        actualSize += nRead;
                    }
                }
                catch (Exception) { }
                if (fs != null)
                {
                    try { fs.Close(); }
                    catch (Exception) { }
                    try { fs.Dispose(); }
                    catch (Exception) { }
                }

                offset += partSize;
            }

            return true;
        }

        private void ThreadSrcReader(object threadParam)
        {
            var idx = (int)threadParam;
            while (true)
            {
                var blockIdx = this._arWorkerReaderBlockIdx[idx];
                if (blockIdx >= 0)
                {
                    this._arWorkerReaderBlockIdx[idx] = -1;
                    this.RunSrcReader(idx, blockIdx);
                    Interlocked.Decrement(ref this._workerReport);
                    continue;
                }
                Thread.Sleep(10);
            }
        }

        private void PrepareWorkerThreads()
        {
            this._workerCount = 0;
            var blockSize = this._db.RaidHeader.BlockSize;
            var n = this._arActualStorageRootPaths.Length;
            this._arWorkerReaderBlockIdx = new int[n];
            this._arWorkerBuf = new byte[n][];
            for (int i = 0; i < n; ++i)
            {
                var buf = new byte[blockSize];
                ZeroBuf(buf, 0, buf.Length);

                this._arWorkerReaderBlockIdx[i] = -1;
                this._arWorkerBuf[i] = buf;

                if (!this._arActualStorageChecking[i])
                {
                    continue;
                }

                var thread = new Thread(this.ThreadSrcReader);
                thread.IsBackground = true;
                thread.Start(i);

                ++this._workerCount;

#if DEBUG
                //break;
#endif
            }
        }

        private void PrepareActualStorageRootPaths()
        {
            var storages = this._db.Storages;
            var n = storages.Length;
            this._arActualStorageRootPaths = new string[n];
            this._arActualStorageChecking = new bool[n];
            for (int i = 0; i < n; ++i)
            {
                var storage = storages[i];
                var path = GetStorageActualPath(storage);
                this._arActualStorageRootPaths[i] = path;

                var storageAlive = !string.IsNullOrEmpty(path);
                if (storageAlive)
                {
                    storageAlive = Directory.Exists(path);
                }
                this._arActualStorageChecking[i] = storageAlive;

                this._dicStorageNum2Idx[storage.StorageNumber] = i;
            }
        }

        private int FindStorageIndex(uint storageNumber)
        {
            var storages = this._db.Storages;
            var n = storages.Length;
            for (int i = 0; i < n; ++i)
            {
                var storage = storages[i];
                if (storageNumber == storage.StorageNumber)
                {
                    return i;
                }
            }
            return -1;
        }

        private static string ResolveStorageRootPath(DirectoryRaid.StorageNode storage)
        {
            var label = storage.Name;
            var drives = DriveInfo.GetDrives();
            foreach (var drive in drives)
            {
                if (drive.VolumeLabel.Equals(label, StringComparison.OrdinalIgnoreCase))
                {
                    return drive.RootDirectory.FullName;
                }
            }
            return null;
        }

        private static string GetStorageActualPath(DirectoryRaid.RaidDB.StorageNode storage)
        {
            var label = storage.Name;
            var drives = DriveInfo.GetDrives();
            foreach (var drive in drives)
            {
                if (drive.VolumeLabel.Equals(label, StringComparison.OrdinalIgnoreCase))
                {
                    var path = drive.RootDirectory.FullName;
                    if (string.IsNullOrEmpty(path))
                    {
                        return null;
                    }

                    path = Path.Combine(path, storage.RelativePath);
                    return path;
                }
            }
            return null;
        }

        //
    }
}
