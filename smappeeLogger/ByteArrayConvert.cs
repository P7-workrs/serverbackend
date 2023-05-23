using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace smappeeLogger
{
    public static class ByteArrayConvert
    {
        // Convert byte array to hex string
        public static String ByteArrayToHexString(Byte[] ArrayToConvert, String Delimiter)
        {
            String[] BATHS = { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F", "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF", "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF", "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF", "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF", "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF", "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF" };
            Int32 LengthRequired = (ArrayToConvert.Length + Delimiter.Length) * 2;
            StringBuilder tempstr = new StringBuilder(LengthRequired, LengthRequired);
            foreach (Byte CurrentElem in ArrayToConvert)
            {
                tempstr.Append(BATHS[CurrentElem]);
                tempstr.Append(Delimiter);
            }

            return tempstr.ToString();
        }

        public static SByte ToInt8(Byte[] bytes, Int32 idx)
        {
            SByte i8 = 0;
            i8 = (SByte)bytes[idx];
            return i8;
        }

        public static Int16 ToInt16(Byte[] bytes, Int32 idx)
        {
            return (Int16)ToUInt16(bytes, idx);
        }

        public static Int16 ToInt16LittleIndian(Byte[] bytes, Int32 idx)
        {
            return (Int16)ToUInt16LittleIndian(bytes, idx);
        }

        public static Int32 ToInt24LittleEndian(Byte[] bytes, Int32 idx)
        {
            Byte[] a = new Byte[4];
            a[0] = bytes[3];
            a[1] = bytes[2];
            a[2] = bytes[1];
            a[3] = bytes[0];
            return (Int32)ToUInt24(a, idx);
        }

        public static Int32 ToInt24(Byte[] bytes, Int32 idx)
        {
            return (Int32)ToUInt24(bytes, idx);
        }

        public static Int32 ToInt32(Byte[] bytes, Int32 idx)
        {
            return (Int32)ToUInt32(bytes, idx);
        }

        public static Int64 ToInt48LittleEndian(Byte[] bytes, Int32 idx)
        {
            Byte[] a = new Byte[6];
            a[0] = bytes[idx + 5];
            a[1] = bytes[idx + 4];
            a[2] = bytes[idx + 3];
            a[3] = bytes[idx + 2];
            a[4] = bytes[idx + 1];
            a[5] = bytes[idx];
            return (Int64)ToUInt48(a, 0);
        }

        public static Int64 ToInt48(Byte[] bytes, Int32 idx)
        {
            return (Int64)ToUInt48(bytes, idx);
        }

        public static Int64 ToInt64(Byte[] bytes, Int32 idx)
        {
            return (Int64)ToUInt64(bytes, idx);
        }

        public static UInt16 ToUInt16(Byte[] bytes, Int32 idx)
        {
            UInt32 u32 = 0;
            u32 += (UInt32)bytes[idx++] << 8;
            u32 += (UInt32)bytes[idx] << 0;
            return (UInt16)u32;
        }

        public static UInt16 ToUInt16LittleIndian(Byte[] bytes, Int32 idx)
        {
            UInt32 u32 = 0;
            u32 += (UInt32)bytes[idx++] << 8;
            u32 += (UInt32)bytes[idx] << 0;
            return (UInt16)u32;
        }

        public static UInt32 ToUInt24(Byte[] bytes, Int32 idx)
        {
            UInt32 u32 = 0;
            u32 += (UInt32)bytes[idx++] << 0;
            u32 += (UInt32)bytes[idx++] << 8;
            u32 += (UInt32)bytes[idx] << 16;
            return u32;
        }

        public static UInt32 ToUInt32(Byte[] bytes, Int32 idx)
        {
            UInt32 u32 = 0;
            u32 += (UInt32)bytes[idx++] << 24;
            u32 += (UInt32)bytes[idx++] << 16;
            u32 += (UInt32)bytes[idx++] << 8;
            u32 += (UInt32)bytes[idx] << 0;
            return u32;
        }

        public static UInt64 ToUInt48(Byte[] bytes, Int32 idx)
        {
            UInt64 u64 = 0;
            u64 += (UInt64)bytes[idx++] << 40;
            u64 += (UInt64)bytes[idx++] << 32;
            u64 += (UInt64)bytes[idx++] << 24;
            u64 += (UInt64)bytes[idx++] << 16;
            u64 += (UInt64)bytes[idx++] << 8;
            u64 += (UInt64)bytes[idx] << 0;
            return u64;
        }

        public static UInt64 ToUInt64(Byte[] bytes, Int32 idx)
        {
            UInt64 u64 = 0;
            u64 += (UInt64)bytes[idx++] << 56;
            u64 += (UInt64)bytes[idx++] << 48;
            u64 += (UInt64)bytes[idx++] << 40;
            u64 += (UInt64)bytes[idx++] << 32;
            u64 += (UInt64)bytes[idx++] << 24;
            u64 += (UInt64)bytes[idx++] << 16;
            u64 += (UInt64)bytes[idx++] << 8;
            u64 += (UInt64)bytes[idx] << 0;
            return u64;
        }

        public static Single ToSingle(Byte[] bytes, Int32 idx)
        {
            Byte[] val = new Byte[4];
            val[0] = bytes[3 + idx];
            val[1] = bytes[2 + idx];
            val[2] = bytes[1 + idx];
            val[3] = bytes[0 + idx];

            return BitConverter.ToSingle(val, 0);
        }

        public static Single ToSingleIskraT6(Byte[] bytes, Int32 idx)
        {
            Byte[] val = new Byte[4];
            val[0] = bytes[1 + idx];
            val[1] = bytes[2 + idx];
            val[2] = bytes[3 + idx];
            val[3] = 0;

            Int32 i32 = ToInt32(val, 0);
            Single value = i32 / 256;
            SByte exp = (SByte)bytes[0 + idx];

            value = value * (Single)System.Math.Pow(10, exp);

            return value;
        }
    }
}
