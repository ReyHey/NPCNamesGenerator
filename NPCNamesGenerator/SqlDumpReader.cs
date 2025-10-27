using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

internal static class SqlDumpReader
{
    internal sealed class DumpData
    {
        public Dictionary<int, string> idToEn = new();
        public Dictionary<int, Dictionary<string, string>> idToLoc = new();
    }

    public static async Task<DumpData> ParseDumpAsync(string path)
    {
        var data = new DumpData();
        using var fs = File.OpenRead(path);
        using var sr = new StreamReader(fs, Encoding.UTF8, true, 1 << 20);

        string? line;
        var sb = new StringBuilder(1 << 20);
        bool inInsert = false;

        string? currentTable = null;
        List<string>? currentCols = null;

        while ((line = await sr.ReadLineAsync()) != null)
        {
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("INSERT INTO", StringComparison.OrdinalIgnoreCase))
            {
                sb.Clear();
                sb.Append(line).Append('\n');
                inInsert = true;

                // capture "INSERT INTO `table` (...) VALUES" or "INSERT INTO world.table VALUES"
                var m = Regex.Match(
                    line,
                    @"INSERT\s+INTO\s+(?:`?\w+`?\.)?`?(\w+)`?\s*(?:\(([^)]*)\))?",
                    RegexOptions.IgnoreCase
                );
                if (!m.Success)
                {
                    inInsert = false;
                    continue;
                }

                currentTable = m.Groups[1].Value;
                if (m.Groups[2].Success && !string.IsNullOrWhiteSpace(m.Groups[2].Value))
                {
                    currentCols = m.Groups[2].Value
                        .Split(',')
                        .Select(s => s.Trim().Trim('`', ' '))
                        .ToList();
                }
                else
                {
                    // handle tables with no column list
                    if (currentTable.Equals("creature_template_locale", StringComparison.OrdinalIgnoreCase))
                    {
                        currentCols = new List<string> { "ID", "locale", "Name", "NameAlt", "Title", "TitleAlt", "VerifiedBuild" };
                    }
                    else if (currentTable.Equals("locales_creature", StringComparison.OrdinalIgnoreCase))
                    {
                        // fallback columns (we’ll look for name_loc1..10 later)
                        currentCols = new List<string> { "entry" };
                    }
                    else if (currentTable.Equals("creature_template", StringComparison.OrdinalIgnoreCase))
                    {
                        // only need entry + name
                        currentCols = new List<string> { "entry", "name" };
                    }
                    else
                    {
                        currentCols = null; // skip unknowns
                    }
                }

                // Read until end of this multi-line insert
                while (inInsert && (line = await sr.ReadLineAsync()) != null)
                {
                    sb.Append(line).Append('\n');
                    if (line.TrimEnd().EndsWith(";"))
                    {
                        inInsert = false;
                        var insertSql = sb.ToString();
                        if (currentCols != null)
                            ProcessInsert(insertSql, currentTable!, currentCols!, data);
                    }
                }
            }
        }

        return data;
    }

    private static void ProcessInsert(string sql, string table, List<string> cols, DumpData data)
    {
        var idx = sql.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase);
        if (idx < 0) return;
        var valuesPart = sql[(idx + "VALUES".Length)..];

        foreach (var tuple in SplitTuples(valuesPart))
        {
            var values = SplitFields(tuple).ToList();
            if (values.Count < cols.Count)
                continue;

            if (table.Equals("creature_template", StringComparison.OrdinalIgnoreCase))
            {
                var idIdx = IndexOf(cols, "entry", "Id", "ID");
                var nameIdx = IndexOf(cols, "name", "Name");
                if (idIdx < 0 || nameIdx < 0) continue;

                var id = ToInt(values[idIdx]);
                var name = Unquote(values[nameIdx]);
                if (id != null && !string.IsNullOrEmpty(name))
                    data.idToEn[id.Value] = name!;
            }
            else if (table.Equals("creature_template_locale", StringComparison.OrdinalIgnoreCase))
            {
                var idIdx = IndexOf(cols, "ID", "Id", "entry");
                var locIdx = IndexOf(cols, "locale", "Locale");
                var nameIdx = IndexOf(cols, "Name", "name");
                if (idIdx < 0 || locIdx < 0 || nameIdx < 0) continue;

                var id = ToInt(values[idIdx]);
                var loc = Unquote(values[locIdx]);
                var name = Unquote(values[nameIdx]);
                if (id != null && !string.IsNullOrEmpty(loc) && !string.IsNullOrEmpty(name))
                {
                    var map = GetLocMap(data.idToLoc, id.Value);
                    map[loc!] = name!;
                }
            }
            else if (table.Equals("locales_creature", StringComparison.OrdinalIgnoreCase))
            {
                var idIdx = IndexOf(cols, "entry", "ID", "Id");
                if (idIdx < 0) continue;
                var id = ToInt(values[idIdx]);
                if (id == null) continue;

                var map = GetLocMap(data.idToLoc, id.Value);
                for (int i = 0; i < cols.Count; i++)
                {
                    var col = cols[i];
                    if (col.StartsWith("name_loc", StringComparison.OrdinalIgnoreCase))
                    {
                        var idxStr = col.Substring("name_loc".Length);
                        if (int.TryParse(idxStr, out var locIdx))
                        {
                            var code = LocIdxToCode(locIdx);
                            if (code != null)
                            {
                                var v = Unquote(values[i]);
                                if (!string.IsNullOrEmpty(v))
                                    map[code] = v!;
                            }
                        }
                    }
                }
            }
        }
    }

    private static Dictionary<string, string> GetLocMap(Dictionary<int, Dictionary<string, string>> dict, int id)
    {
        if (!dict.TryGetValue(id, out var m))
        {
            m = new(StringComparer.Ordinal);
            dict[id] = m;
        }
        return m;
    }

    private static int? ToInt(string token)
    {
        token = token.Trim();
        if (token.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return null;
        if (int.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return v;
        var uq = Unquote(token);
        if (int.TryParse(uq, NumberStyles.Integer, CultureInfo.InvariantCulture, out v)) return v;
        return null;
    }

    private static string? Unquote(string token)
    {
        token = token.Trim();
        if (token.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return null;
        if (token.Length >= 2 && token[0] == '\'' && token[^1] == '\'')
        {
            var inner = token[1..^1];
            return inner.Replace("\\'", "'").Replace("\\\\", "\\");
        }
        return token;
    }

    private static int IndexOf(List<string> cols, params string[] names)
    {
        for (int i = 0; i < cols.Count; i++)
            foreach (var n in names)
                if (string.Equals(cols[i], n, StringComparison.OrdinalIgnoreCase))
                    return i;
        return -1;
    }

    private static IEnumerable<string> SplitTuples(string s)
    {
        var buf = new StringBuilder();
        int depth = 0;
        bool inStr = false, esc = false;

        for (int i = 0; i < s.Length; i++)
        {
            var c = s[i];
            buf.Append(c);

            if (inStr)
            {
                if (esc) { esc = false; continue; }
                if (c == '\\') { esc = true; continue; }
                if (c == '\'') inStr = false;
                continue;
            }

            if (c == '\'') { inStr = true; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;

            if (depth == 0 && c == ')')
            {
                var t = buf.ToString();
                t = t.Trim().TrimEnd(',', ';');
                var open = t.IndexOf('(');
                if (open >= 0)
                    yield return t[(open + 1)..];
                buf.Clear();
            }
        }
    }

    private static IEnumerable<string> SplitFields(string tuple)
    {
        var sb = new StringBuilder();
        bool inStr = false, esc = false;
        for (int i = 0; i < tuple.Length; i++)
        {
            var c = tuple[i];
            if (inStr)
            {
                sb.Append(c);
                if (esc) { esc = false; continue; }
                if (c == '\\') { esc = true; continue; }
                if (c == '\'') inStr = false;
            }
            else
            {
                if (c == '\'') { inStr = true; sb.Append(c); }
                else if (c == ',') { yield return sb.ToString().Trim(); sb.Clear(); }
                else sb.Append(c);
            }
        }
        yield return sb.ToString().Trim();
    }

    private static string? LocIdxToCode(int idx) => idx switch
    {
        1 => "koKR", 2 => "frFR", 3 => "deDE", 4 => "zhCN", 5 => "zhTW",
        6 => "esES", 7 => "esMX", 8 => "ruRU", 9 => "ptBR", 10 => "itIT",
        _ => null
    };
}
