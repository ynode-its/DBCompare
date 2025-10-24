using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Odbc;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace DBCompare
{
    class Program
    {
        static string logFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"compare_log_{DateTime.Now:yyyyMMdd_HHmmss}.txt");

        static void Main()
        {
            var section = (DatabaseSettingsSection)ConfigurationManager.GetSection("databaseSettings");
            string conOld = section.Connections["oldDB"].ConnectionString;
            string conNew = section.Connections["newDB"].ConnectionString;

            var excludePatterns = section.ExcludeTables
                .Cast<ExcludeTableElement>()
                .Select(e => WildcardToRegex(e.Name))
                .ToList();

            Console.WriteLine("=== SQL Server データ比較ツール（ワイルドカード対応・低メモリ・ログ付き） ===\n");
            File.AppendAllText(logFile, $"=== 実行開始 {DateTime.Now} ==={Environment.NewLine}");

            var tables = GetUserTables(conNew);
            long totalDiff = 0;
            int index = 0;

            foreach (var (schema, table) in tables)
            {
                string fullName = $"{schema}.{table}";
                if (IsExcluded(fullName, excludePatterns))
                {
                    Console.WriteLine($"[{index + 1}/{tables.Count}] {fullName} → 除外");
                    File.AppendAllText(logFile, $"除外: {fullName}{Environment.NewLine}");
                    continue;
                }

                index++;
                Console.WriteLine($"[{index}/{tables.Count}] {fullName} 比較中...");
                File.AppendAllText(logFile, $"[{index}] {fullName} 比較開始: {DateTime.Now}\n");

                try
                {
                    long diff = CompareTables(conOld, conNew, schema, table);
                    totalDiff += diff;
                    Console.WriteLine($" → 不一致件数: {diff:N0}");
                    File.AppendAllText(logFile, $"結果: {diff} 件不一致\n");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($" ⚠ {fullName} エラー: {ex.Message}");
                    File.AppendAllText(logFile, $"エラー: {fullName} - {ex.Message}\n");
                }

                File.AppendAllText(logFile, Environment.NewLine);
            }

            Console.WriteLine($"✅ 全テーブル不一致件数合計: {totalDiff:N0}");
            File.AppendAllText(logFile, $"=== 実行終了 {DateTime.Now} ==={Environment.NewLine}");
        }

        // ============================================================
        //  除外判定
        // ============================================================

        static bool IsExcluded(string tableFullName, List<Regex> patterns)
        {
            return patterns.Any(r => r.IsMatch(tableFullName));
        }

        static Regex WildcardToRegex(string wildcard)
        {
            // SQL LIKE 形式（%と_）を .NET 正規表現に変換
            string escaped = Regex.Escape(wildcard);
            string pattern = "^" + escaped
                .Replace("%", ".*")   // ← ここを修正！（@"\%" ではなく "%"）
                .Replace("_", ".") + "$";

            return new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);
        }

        // ============================================================
        //  SQL 比較ロジック
        // ============================================================

        static List<(string Schema, string Table)> GetUserTables(string connStr)
        {
            var list = new List<(string, string)>();
            string sql = @"
SELECT s.name AS SchemaName, t.name AS TableName
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name;";

            using (var conn = new OdbcConnection(connStr))
            using (var cmd = new OdbcCommand(sql, conn))
            {
                conn.Open();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        list.Add((reader.GetString(0), reader.GetString(1)));
                }
            }
            return list;
        }

        static List<string> GetColumns(string connStr, string schema, string table)
        {
            var cols = new List<string>();
            string sql = @"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION;";

            using (var conn = new OdbcConnection(connStr))
            using (var cmd = new OdbcCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@p1", schema);
                cmd.Parameters.AddWithValue("@p2", table);
                conn.Open();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        cols.Add(reader.GetString(0));
                }
            }
            return cols;
        }

        static long CompareTables(string conOld, string conNew, string schema, string table)
        {
            var cols = GetColumns(conNew, schema, table);
            if (cols.Count == 0) return 0;

            string concatExpr = cols.Count == 1
                ? $"ISNULL(CONVERT(NVARCHAR(MAX), {cols[0]}), '')"
                : $"CONCAT({string.Join(", ", cols.Select(c => $"ISNULL(CONVERT(NVARCHAR(MAX), {c}), '')"))})";

            string sql = $"SELECT HASHBYTES('SHA2_256', {concatExpr}) AS HashVal FROM [{schema}].[{table}]";

            string tmpNew = Path.Combine(Path.GetTempPath(), $"new_hash_{schema}_{table}.txt");
            string tmpOld = Path.Combine(Path.GetTempPath(), $"old_hash_{schema}_{table}.txt");

            using (var conn = new OdbcConnection(conNew))
            using (var cmd = new OdbcCommand(sql, conn))
            {
                conn.Open();
                using (var reader = cmd.ExecuteReader())
                using (var writer = new StreamWriter(tmpNew, false))
                {
                    while (reader.Read())
                    {
                        var bytes = (byte[])reader["HashVal"];
                        writer.WriteLine(Convert.ToBase64String(bytes));
                    }
                }
            }

            using (var conn = new OdbcConnection(conOld))
            using (var cmd = new OdbcCommand(sql, conn))
            {
                conn.Open();
                using (var reader = cmd.ExecuteReader())
                using (var writer = new StreamWriter(tmpOld, false))
                {
                    while (reader.Read())
                    {
                        var bytes = (byte[])reader["HashVal"];
                        writer.WriteLine(Convert.ToBase64String(bytes));
                    }
                }
            }

            var newSet = new HashSet<string>(File.ReadLines(tmpNew));
            long diffCount = 0;

            foreach (var line in File.ReadLines(tmpOld))
            {
                if (!newSet.Contains(line))
                    diffCount++;
            }

            try
            {
                File.Delete(tmpNew);
                File.Delete(tmpOld);
            }
            catch { }

            return diffCount;
        }
    }
}
