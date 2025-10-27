using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

internal static class Program
{
    private static readonly string[] DEFAULT_LOCALES =
        { "deDE","esES","frFR","itIT","koKR","ptBR","ruRU","zhCN","zhTW" };

    public static async Task<int> Main(string[] args)
    {
        var root = new System.CommandLine.RootCommand("Rebuild Locale/NPCNames.lua by parsing SQL dumps (no DB).");

        var wotlkSql  = new Option<string>("--wotlk-sql", "Path to 3.3.5 SQL dump (.sql)") { IsRequired = true };
        var retailSql = new Option<string>("--retail-sql", "Path to 11.x SQL dump (.sql)")   { IsRequired = true };
        var seedPath  = new Option<string?>("--seed", () => null, "Seed file: JSON list/object or existing NPCNames.lua");
        var locales   = new Option<string[]>("--locales", () => DEFAULT_LOCALES, "Locales to emit");
        var minFuzzy  = new Option<int>("--min-fuzzy", () => 86, "Min fuzzy score [0-100]");
        var outPath   = new Option<string>("--out", () => Path.Combine("Locale","NPCNames.lua"), "Output Lua path");

        root.Add(wotlkSql); root.Add(retailSql); root.Add(seedPath); root.Add(locales); root.Add(minFuzzy); root.Add(outPath);

        root.SetHandler(async (w335, w112, seed, locs, minF, outp) =>
        {
            await RunAsync(w335, w112, seed, locs, minF, outp);
        }, wotlkSql, retailSql, seedPath, locales, minFuzzy, outPath);

        return await root.InvokeAsync(args);
    }

    private static async Task RunAsync(string w335Path, string w112Path, string? seedPath, string[] locales, int minFuzzy, string outputPath)
    {
        Console.WriteLine("Parsing WotLK (3.3.5) dump...");
        var (ct335, loc335) = await SqlDumpReader.ParseDumpAsync(w335Path);

        Console.WriteLine("Parsing Retail (11.x) dump...");
        var (ct112, loc112) = await SqlDumpReader.ParseDumpAsync(w112Path);

        Console.WriteLine("Merging sources...");
        var (idToEn, idToLoc) = MergeSources(ct112.idToEn, ct112.idToLoc, ct335.idToEn, ct335.idToLoc, locales);

        // Index EN name -> IDs
        var enToIds = new Dictionary<string,List<int>>(StringComparer.Ordinal);
        foreach (var kv in idToEn)
        {
            if (!enToIds.TryGetValue(kv.Value, out var list)) { list = new List<int>(); enToIds[kv.Value] = list; }
            list.Add(kv.Key);
        }

        // Targets
        var targets = seedPath != null ? LoadSeed(seedPath) : idToEn.Values.Distinct().OrderBy(s=>s, StringComparer.Ordinal).ToList();

        // Normalized index
        var normIndex = new Dictionary<string, HashSet<string>>();
        foreach (var en in enToIds.Keys)
        {
            foreach (var key in new[]{ Norm(en), Norm(MaybeSingular(en)) })
            {
                if (!normIndex.TryGetValue(key, out var set)) { set = new HashSet<string>(StringComparer.Ordinal); normIndex[key] = set; }
                set.Add(en);
            }
        }

        var results = new SortedDictionary<string, Dictionary<string,string>>(StringComparer.Ordinal);
        var notFound = new List<string[]>();
        var conflicts = new List<string[]>();

        Console.WriteLine($"Matching {targets.Count} seed names...");
        foreach (var seed in targets)
        {
            var matchedEn = FindBestMatch(seed, enToIds.Keys, normIndex, minFuzzy);
            if (matchedEn == null)
            {
                notFound.Add(new[]{ seed, "", "no-match" });
                continue;
            }

            var ids = enToIds[matchedEn];
            int chosen = ids.Count == 1
                ? ids[0]
                : ids.Select(i => (i, idToLoc.TryGetValue(i, out var m) ? m.Count : 0))
                     .OrderByDescending(t => t.Item2).First().i;

            if (ids.Count > 1)
                conflicts.Add(new[]{ seed, matchedEn, $"multiple IDs: {string.Join(",", ids)}" });

            var locMap = new Dictionary<string,string>(StringComparer.Ordinal);
            if (idToLoc.TryGetValue(chosen, out var m))
            {
                foreach (var code in locales)
                    if (m.TryGetValue(code, out var val) && !string.IsNullOrWhiteSpace(val))
                        locMap[code] = val;
            }
            if (locMap.Count == 0)
                notFound.Add(new[]{ seed, matchedEn, "no-locales" });

            results[seed] = locMap;
        }

        Console.WriteLine("Writing outputs...");
        WriteLua(outputPath, results);
        WriteCsv("not_found.csv", new[]{"seed_name","matched_en","reason"}, notFound);
        WriteCsv("conflicts.csv", new[]{"seed_name","matched_en","details"}, conflicts);

        var stats = $@"Seeds: {targets.Count}
Resolved: {results.Count}
No locales: {notFound.Count(r => r[2] == "no-locales")}
No match: {notFound.Count(r => r[2] == "no-match")}
Conflicts: {conflicts.Count}
";
        File.WriteAllText("stats.txt", stats);

        Console.WriteLine("Done.");
    }

    // Merge: prefer 11.x, fill with 3.3.5
    private static (Dictionary<int,string>, Dictionary<int,Dictionary<string,string>>)
        MergeSources(Dictionary<int,string> pEn, Dictionary<int,Dictionary<string,string>> pLoc,
                     Dictionary<int,string> sEn, Dictionary<int,Dictionary<string,string>> sLoc,
                     string[] allowedLocales)
    {
        var allIds = new HashSet<int>(pEn.Keys); allIds.UnionWith(sEn.Keys);
        var idToEn = new Dictionary<int,string>();
        var idToLoc = new Dictionary<int,Dictionary<string,string>>();
        foreach (var id in allIds)
        {
            var en = pEn.TryGetValue(id, out var pe) ? pe : (sEn.TryGetValue(id, out var se) ? se : null);
            if (en != null) idToEn[id] = en;

            if (pLoc.TryGetValue(id, out var pl)) idToLoc[id] = new(pl);
            if (sLoc.TryGetValue(id, out var sl))
            {
                if (!idToLoc.TryGetValue(id, out var cur)) { cur = new(StringComparer.Ordinal); idToLoc[id] = cur; }
                foreach (var kv in sl)
                    if (Array.IndexOf(allowedLocales, kv.Key) >= 0 && !cur.ContainsKey(kv.Key))
                        cur[kv.Key] = kv.Value;
            }

            // prune locales to allowed set
            if (idToLoc.TryGetValue(id, out var m))
            {
                foreach (var k in m.Keys.ToList())
                    if (Array.IndexOf(allowedLocales, k) < 0)
                        m.Remove(k);
            }
        }
        return (idToEn, idToLoc);
    }

    private static List<string> LoadSeed(string path)
    {
        var ext = Path.GetExtension(path).ToLowerInvariant();
        var text = File.ReadAllText(path, new UTF8Encoding(false));
        if (ext == ".json")
        {
            try {
                var list = JsonSerializer.Deserialize<List<string>>(text);
                if (list != null) return list.Where(s=>!string.IsNullOrWhiteSpace(s)).Distinct().OrderBy(s=>s).ToList();
            } catch { /* ignore */ }
            var dict = JsonSerializer.Deserialize<Dictionary<string, object>>(text);
            if (dict != null) return dict.Keys.Distinct().OrderBy(s=>s).ToList();
            throw new Exception("Unsupported JSON seed structure");
        }
        if (ext == ".lua")
        {
            var rx = new Regex("\\[\\s*\"(.+?)\"\\s*]\\s*=", RegexOptions.Compiled);
            return rx.Matches(text).Select(m => m.Groups[1].Value).Distinct().OrderBy(s=>s).ToList();
        }
        throw new Exception($"Unsupported seed file: {path}");
    }

    private static void WriteLua(string path, SortedDictionary<string,Dictionary<string,string>> data)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.WriteLine("-- Auto-generated by RebuildNpcNames (C# no-DB)\nRXPData = RXPData or {}\nRXPData.NPCNameDB = {");
        foreach (var kv in data)
        {
            var en = LuaEscape(kv.Key);
            var parts = string.Join(", ", kv.Value.OrderBy(p=>p.Key, StringComparer.Ordinal)
                .Select(p => $"{p.Key}=\"{LuaEscape(p.Value)}\""));
            w.WriteLine($"  [\"{en}\"] = {{ {parts} }},");
        }
        w.WriteLine("}");
    }
    private static string LuaEscape(string s) => s.Replace("\\","\\\\").Replace("\"","\\\"");

    private static void WriteCsv(string path, string[] header, List<string[]> rows)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.WriteLine(string.Join(",", header));
        foreach (var r in rows) w.WriteLine(string.Join(",", r.Select(EscCsv)));
        static string EscCsv(string v) => (v.Contains('"') || v.Contains(',') || v.Contains('\n')) ? $"\"{v.Replace("\"","\"\"")}\"" : v;
    }

    // Matching helpers ------------------------------------------------------
    private static string? FindBestMatch(string seed, IEnumerable<string> all, Dictionary<string,HashSet<string>> normIndex, int minFuzzy)
    {
        if (all.Contains(seed)) return seed;
        var k1 = Norm(seed);
        if (normIndex.TryGetValue(k1, out var set) && set.Count == 1) return set.First();
        var cand = new HashSet<string>(StringComparer.Ordinal);
        if (normIndex.TryGetValue(k1, out var s1)) foreach (var s in s1) cand.Add(s);
        var k2 = Norm(MaybeSingular(seed));
        if (normIndex.TryGetValue(k2, out var s2)) foreach (var s in s2) cand.Add(s);
        if (cand.Count == 0) cand = new HashSet<string>(all);

        string? best = null; int bestScore = -1;
        foreach (var c in cand)
        {
            var score = WRatio(seed, c);
            if (score > bestScore) { best = c; bestScore = score; }
        }
        return bestScore >= minFuzzy ? best : null;
    }
    private static int WRatio(string a, string b)
    { var r1 = Ratio(a,b); var r2 = Ratio(Norm(a),Norm(b)); return Math.Max(r1,r2); }
    private static int Ratio(string a,string b)
    { int lev = Levenshtein(a,b); int maxLen = Math.Max(1, Math.Max(a.Length,b.Length)); return (int)Math.Round((1.0 - (double)lev / maxLen)*100); }
    private static int Levenshtein(string s,string t)
    { int n=s.Length,m=t.Length; var d=new int[n+1,m+1]; for(int i=0;i<=n;i++) d[i,0]=i; for(int j=0;j<=m;j++) d[0,j]=j;
      for(int i=1;i<=n;i++) for(int j=1;j<=m;j++){ int cost=s[i-1]==t[j-1]?0:1; d[i,j]=Math.Min(Math.Min(d[i-1,j]+1,d[i,j-1]+1),d[i-1,j-1]+cost);} return d[n,m]; }
    private static string Norm(string s)
    { if (string.IsNullOrEmpty(s)) return s; s=s.Normalize(NormalizationForm.FormKC).Replace('’','\'').Replace('–','-').Replace('—','-');
      s=Regex.Replace(s, "\\s*\\([^)]*\\)", ""); s=Regex.Replace(s,"\\s+"," ").Trim(); return s.ToLowerInvariant(); }
    private static string MaybeSingular(string s)
    { var parts=s.Split(' ',StringSplitOptions.RemoveEmptyEntries); if(parts.Length==0)return s; var last=parts[^1]; if(last.Length>3 && last.EndsWith("s")) parts[^1]=last[..^1]; return string.Join(' ',parts); }
}

// --------- SQL Dump Reader (streaming, no DB) ------------------------------

internal static class SqlDumpReader
{
    // Result for one dump
    internal sealed class DumpData
    {
        public Dictionary<int,string> idToEn = new();
        public Dictionary<int,Dictionary<string,string>> idToLoc = new();
    }

    // Entry points
    public static async Task<(DumpData, DumpData)> ParseDumpAsync(string path)
    {
        // We parse one file and return a DumpData; to keep tuple signature, return (data, data)
        var data = await ParseSingleAsync(path);
        return (data, data); // not used; kept for compatibility with earlier signature
    }

    // Parse one dump (handles both schemas)
    public static async Task<DumpData> ParseSingleAsync(string path)
    {
        var data = new DumpData();
        using var fs = File.OpenRead(path);
        using var sr = new StreamReader(fs, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1<<20);

        string? line;
        var sb = new StringBuilder(1<<20);
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

                // detect table and columns
                var m = Regex.Match(line, @"INSERT\s+INTO\s+`?(\w+)`?\s*\(([^)]+)\)", RegexOptions.IgnoreCase);
                if (!m.Success) { inInsert = false; continue; }
                currentTable = m.Groups[1].Value;
                currentCols = m.Groups[2].Value.Split(',').Select(s => s.Trim().Trim('`',' ')).ToList();

                // Continue reading until we hit a semicolon (end of multi-row insert)
                while (inInsert && (line = await sr.ReadLineAsync()) != null)
                {
                    sb.Append(line).Append('\n');
                    if (line.TrimEnd().EndsWith(";"))
                    {
                        inInsert = false;
                        var insertSql = sb.ToString();
                        ProcessInsert(insertSql, currentTable!, currentCols!, data);
                    }
                }
            }
        }
        return data;
    }

    private static void ProcessInsert(string sql, string table, List<string> cols, SqlDumpReader.DumpData data)
    {
        // Pull the VALUES (...) (...) ...; part
        var idx = sql.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase);
        if (idx < 0) return;
        var valuesPart = sql[(idx + "VALUES".Length)..];

        foreach (var tuple in SplitTuples(valuesPart))
        {
            var values = SplitFields(tuple).ToList();
            if (values.Count != cols.Count) continue; // skip malformed

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
                // 11.x format: ID, locale, Name
                var idIdx = IndexOf(cols, "ID","Id","entry");
                var locIdx = IndexOf(cols, "locale","Locale");
                var nameIdx = IndexOf(cols, "Name","name");
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
                // 3.3.5 wide table: entry + name_loc1..10 (we map subset to codes)
                var idIdx = IndexOf(cols, "entry","ID","Id");
                if (idIdx < 0) return;
                var id = ToInt(values[idIdx]);
                if (id == null) return;

                var map = GetLocMap(data.idToLoc, id.Value);
                for (int i=0;i<cols.Count;i++)
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
                                if (!string.IsNullOrEmpty(v)) map[code] = v!;
                            }
                        }
                    }
                }
            }
        }
    }

    private static Dictionary<string,string> GetLocMap(Dictionary<int,Dictionary<string,string>> dict, int id)
    {
        if (!dict.TryGetValue(id, out var m)) { m = new(StringComparer.Ordinal); dict[id] = m; }
        return m;
    }

    private static int? ToInt(string token)
    {
        token = token.Trim();
        if (token.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return null;
        if (int.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return v;
        // sometimes quoted numeric
        var uq = Unquote(token);
        if (int.TryParse(uq, NumberStyles.Integer, CultureInfo.InvariantCulture, out v)) return v;
        return null;
    }

    private static string? Unquote(string token)
    {
        token = token.Trim();
        if (token.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return null;
        if (token.Length>=2 && token[0]=='\'' && token[^1]=='\'')
        {
            var inner = token.Substring(1, token.Length-2);
            return inner.Replace("\\'", "'").Replace("\\\\","\\");
        }
        return token;
    }

    private static int IndexOf(List<string> cols, params string[] names)
    {
        for (int i=0;i<cols.Count;i++)
            foreach (var n in names)
                if (string.Equals(cols[i], n, StringComparison.OrdinalIgnoreCase)) return i;
        return -1;
    }

    // Split VALUES list into individual (...) tuples
    private static IEnumerable<string> SplitTuples(string s)
    {
        var buf = new StringBuilder();
        int depth = 0; bool inStr = false; bool esc = false;

        for (int i=0;i<s.Length;i++)
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
            else
            {
                if (c == '\'') { inStr = true; continue; }
                if (c == '(') depth++;
                else if (c == ')') depth--;
                if (depth == 0 && c == ')')
                {
                    // emit tuple
                    var t = buf.ToString().Trim();
                    // strip trailing comma/semicolon
                    t = t.TrimEnd();
                    while (t.Length>0 && (t[^1]==',' || t[^1]==';' || char.IsWhiteSpace(t[^1]))) t = t[..^1];
                    // find last '('
                    var openIdx = t.IndexOf('(');
                    if (openIdx >= 0) t = t.Substring(openIdx+1); // inside (...)
                    yield return t;
                    buf.Clear();
                }
            }
        }
    }

    // Split a single (...) tuple into top-level comma-separated fields
    private static IEnumerable<string> SplitFields(string tupleInner)
    {
        var sb = new StringBuilder();
        bool inStr = false; bool esc = false;
        for (int i=0;i<tupleInner.Length;i++)
        {
            var c = tupleInner[i];
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
        1=>"koKR", 2=>"frFR", 3=>"deDE", 4=>"zhCN", 5=>"zhTW",
        6=>"esES", 7=>"esMX", 8=>"ruRU", 9=>"ptBR", 10=>"itIT",
        _=>null
    };
}
