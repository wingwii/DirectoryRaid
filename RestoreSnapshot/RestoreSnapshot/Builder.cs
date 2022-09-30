using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace RestoreSnapshot
{
    class Builder
    {
        private RaidDB _db = null;
        private int _workerCount = 0;
        private uint _dstStorageNumber = 0;
        private int _dstStorageIdx = -1;
        private Dictionary<int, int> _dicStorageNum2Idx = new Dictionary<int, int>();
        private DirectoryRaid.StorageNode _dstStorage = null;
        private string _dstStoragePath = string.Empty;
        private string[] _arActualStorageRootPaths = null;
        private bool[] _arActualStorageChecking = null;
        private string _builderStatusFileName = null;
        private int[] _arWorkerReaderBlockIdx = null;
        private byte[][] _arWorkerBuf = null;
        private int _workerReport = 0;


        public Builder(RaidDB db)
        {
            this._db = db;
        }

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
            if (!this._arActualStorageChecking[this._dstStorageIdx])
            {
                return false;
            }

            this.PrepareBuilderStatusFileName();

            this.PrepareWorkerThreads();

            var dataBlocks = this._db.DataBlocks;
            var n = dataBlocks.Length;
            for (int i = 0; i < n; ++i)
            {
                this.WakeAllReaders(i);
                this.WaitForAllReaders();
                this.ComputeCurrentRaidBlock();
                this.SaveRaidBlock(dataBlocks[i]);
            }

            return true;
        }

        private void SaveRaidBlock(DirectoryRaid.RaidDataBlock block)
        {
            var buf = this._arWorkerBuf[this._dstStorageIdx];
            var blockSize = buf.Length;
            foreach (var grp in block.Items)
            {
                if (this._dstStorage != grp.Storage)
                {
                    continue;
                }

                long offset = 0;
                foreach (var part in grp.Items)
                {
                    var partSize = part.Size;
                    var fileName = this.PrepareActualFilePath(this._dstStorageIdx, part.DataFile);
                    Console.WriteLine(fileName);
                    
                    FileStream fs = null;
                    try
                    {
                        fs = File.OpenWrite(fileName);
                        fs.Position = part.Offset;
                        fs.Write(buf, (int)offset, (int)partSize);
                    }
                    catch (Exception) { }
                    try { fs.Flush(); }
                    catch (Exception) { }
                    try { fs.Close(); }
                    catch (Exception) { }
                    try { fs.Dispose(); }
                    catch (Exception) { }

                    offset += partSize;
                }
                break;
            }
        }

        private void ComputeCurrentRaidBlock()
        {
            var buf = this._arWorkerBuf[this._dstStorageIdx];
            var blockSize = buf.Length;
            ZeroBuf(buf, 0, blockSize);

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
            fileName = Path.Combine(fileName, "meta");
            try { Directory.CreateDirectory(fileName); }
            catch (Exception) { }
            this._builderStatusFileName = Path.Combine(fileName, "builder.sav");
        }

        private void WakeAllReaders(int blockIdx)
        {
            var n = this._arWorkerReaderBlockIdx.Length;
            for (int i = 0; i < n; ++i)
            {
                this._arWorkerReaderBlockIdx[i] = blockIdx;
            }
            this._workerReport = this._workerCount;
        }

        private void WaitForAllReaders()
        {
            while (this._workerReport > 0)
            {
                Thread.Sleep(10);
            }
        }

        private static DirectoryRaid.FilePartsGroup FindAssocFilePartGroup(DirectoryRaid.RaidDataBlock dataBlock, int storageNumber)
        {
            foreach (var grp in dataBlock.Items)
            {
                if (storageNumber == grp.Storage.StorageNumber)
                {
                    return grp;
                }
            }
            return null;
        }

        private static void ZeroBuf(byte[] buf, int offset, int size)
        {
            for (int i = 0; i < size; ++i)
            {
                buf[offset + i] = 0;
            }
        }

        private string PrepareActualFilePath(int storageIdx, DirectoryRaid.FileNode file)
        {
            var storage = file.Storage;
            var rootDir = storage.RootDirectory as DirectoryRaid.FileNode;

            var stack = new Stack<string>();
            var node = file;
            while (node != null)
            {
                if (node == rootDir)
                {
                    break;
                }

                stack.Push(node.Name);
                node = node.ParentNode as DirectoryRaid.FileNode;
            }

            var path = this._arActualStorageRootPaths[storageIdx];
            while (stack.Count > 0)
            {
                var part = stack.Pop();
                path = Path.Combine(path, part);
            }

            return path;
        }

        private bool RunSrcReader(int readerIdx, int blockIdx)
        {
            var storage = this._db.Storages[readerIdx];
            var dataBlock = this._db.DataBlocks[blockIdx];
            var filePartGrp = FindAssocFilePartGroup(dataBlock, storage.StorageNumber);
            if (null == filePartGrp)
            {
                return false;
            }

            long offset = 0;
            var buf = this._arWorkerBuf[readerIdx];
            ZeroBuf(buf, 0, buf.Length);

            var fileParts = filePartGrp.Items;
            foreach (var part in fileParts)
            {
                var fileName = this.PrepareActualFilePath(readerIdx, part.DataFile);
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
                this._arWorkerReaderBlockIdx[i] = -1;
                this._arWorkerBuf[i] = null;

                if (!this._arActualStorageChecking[i])
                {
                    continue;
                }
                this._arWorkerBuf[i] = new byte[blockSize];

                if (i == this._dstStorageIdx)
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

                var x = Directory.Exists(path);
                this._arActualStorageChecking[i] = x;

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

        private static string GetStorageActualPath(DirectoryRaid.StorageNode storage)
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
