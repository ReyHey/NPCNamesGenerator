using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

internal static class Program
{
    private static readonly string[] DEFAULT_LOCALES =
        { "deDE","esES","frFR","itIT","koKR","ptBR","ruRU","zhCN","zhTW" };

    public static async Task<int> Main(string[] args)
    {
        var opts = ParseArgs(args);

        // Required args
        if (!opts.TryGetValue("--wotlk-sql", out var w335) ||
            !opts.TryGetValue("--retail-sql", out var w112) ||
            !opts.TryGetValue("--out", out var outPath))
        {
            Console.WriteLine(
@"Usage:
  NPCNamesGenerator --wotlk-sql <file.sql> --retail-sql <file.sql> [--seed <file>] [--locales <codes...>] [--min-fuzzy <n>] --out <file.lua>

Examples:
  NPCNamesGenerator --wotlk-sql ""C:\...\TDB_full_world_335.sql"" --retail-sql ""C:\...\TDB_full_world_1120.sql"" --out ""C:\...\npcNames.lua""
  NPCNamesGenerator --wotlk-sql ... --retail-sql ... --seed ""C:\...\NPCNames.lua"" --locales deDE frFR ruRU --out ...");
            return 2;
        }

        // Optional args
        var seed = opts.TryGetValue("--seed", out var s) ? s : null;
        var minFuzzy = (opts.TryGetValue("--min-fuzzy", out var mf) && int.TryParse(mf, out var n)) ? n : 86;
        var locales = opts.TryGetValues("--locales")?.ToArray() ?? DEFAULT_LOCALES;

        try
        {
            await RunAsync(w335!, w112!, seed, locales, minFuzzy, outPath!);
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine("ERROR: " + ex.Message);
            return 1;
        }
    }

    private static async Task RunAsync(string w335Path, string w112Path, string? seedPath, string[] locales, int minFuzzy, string outputPath)
    {
        Console.WriteLine("Parsing WotLK (3.3.5) dump...");
        var ct335 = await SqlDumpReader.ParseDumpAsync(w335Path);

        Console.WriteLine("Parsing Retail (11.x) dump...");
        var ct112 = await SqlDumpReader.ParseDumpAsync(w112Path);

        // Debug counts (optional)
        Console.WriteLine($"WotLK: creatures={ct335.idToEn.Count}, localeIDs={ct335.idToLoc.Count}");
        Console.WriteLine($"Retail: creatures={ct112.idToEn.Count}, localeIDs={ct112.idToLoc.Count}");

        Console.WriteLine("Merging sources...");
        var (idToEn, idToLoc) = MergeSources(ct112.idToEn, ct112.idToLoc, ct335.idToEn, ct335.idToLoc, locales);

        // Index: EN -> IDs
        var enToIds = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (var kv in idToEn)
        {
            if (!enToIds.TryGetValue(kv.Value, out var list)) { list = new(); enToIds[kv.Value] = list; }
            list.Add(kv.Key);
        }

        // Targets (seed or everything)
        var targets = seedPath != null ? LoadSeed(seedPath) :
            idToEn.Values.Distinct().OrderBy(s => s, StringComparer.Ordinal).ToList();

        // Normalized index
        var normIndex = new Dictionary<string, HashSet<string>>();
        foreach (var en in enToIds.Keys)
        {
            foreach (var key in new[] { Norm(en), Norm(MaybeSingular(en)) })
            {
                if (!normIndex.TryGetValue(key, out var set)) { set = new(StringComparer.Ordinal); normIndex[key] = set; }
                set.Add(en);
            }
        }

        var results = new SortedDictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        var notFound = new List<string[]>();
        var conflicts = new List<string[]>();

        Console.WriteLine($"Matching {targets.Count} seed names...");
        foreach (var seed in targets)
        {
            var matchedEn = FindBestMatch(seed, enToIds.Keys, normIndex, minFuzzy);
            if (matchedEn == null)
            {
                notFound.Add(new[] { seed, "", "no-match" });
                continue;
            }

            var ids = enToIds[matchedEn];
            int chosen = ids.Count == 1
                ? ids[0]
                : ids.Select(i => (i, idToLoc.TryGetValue(i, out var m) ? m.Count : 0))
                     .OrderByDescending(t => t.Item2).First().i;

            if (ids.Count > 1)
                conflicts.Add(new[] { seed, matchedEn, $"multiple IDs: {string.Join(",", ids)}" });

            var locMap = new Dictionary<string, string>(StringComparer.Ordinal);
            if (idToLoc.TryGetValue(chosen, out var m))
            {
                foreach (var code in locales)
                    if (m.TryGetValue(code, out var val) && !string.IsNullOrWhiteSpace(val))
                        locMap[code] = val;
            }
            if (locMap.Count == 0)
                notFound.Add(new[] { seed, matchedEn, "no-locales" });

            results[seed] = locMap;
        }

        Console.WriteLine("Writing outputs...");
        WriteLua(outputPath, results);
        WriteCsv(Path.Combine(Path.GetDirectoryName(outputPath) ?? ".", "not_found.csv"),
                 new[] { "seed_name", "matched_en", "reason" }, notFound);
        WriteCsv(Path.Combine(Path.GetDirectoryName(outputPath) ?? ".", "conflicts.csv"),
                 new[] { "seed_name", "matched_en", "details" }, conflicts);

        var stats = $@"Seeds: {targets.Count}
Resolved: {results.Count}
No locales: {notFound.Count(r => r[2] == "no-locales")}
No match: {notFound.Count(r => r[2] == "no-match")}
Conflicts: {conflicts.Count}
";
        File.WriteAllText(Path.Combine(Path.GetDirectoryName(outputPath) ?? ".", "stats.txt"), stats);

        Console.WriteLine("Done.");
    }

    // --- Merge (prefer retail then fill from 3.3.5) ---
    private static (Dictionary<int, string>, Dictionary<int, Dictionary<string, string>>)
        MergeSources(Dictionary<int, string> pEn, Dictionary<int, Dictionary<string, string>> pLoc,
                     Dictionary<int, string> sEn, Dictionary<int, Dictionary<string, string>> sLoc,
                     string[] allowedLocales)
    {
        var allIds = new HashSet<int>(pEn.Keys); allIds.UnionWith(sEn.Keys);
        var idToEn = new Dictionary<int, string>();
        var idToLoc = new Dictionary<int, Dictionary<string, string>>();
        foreach (var id in allIds)
        {
            var en = pEn.TryGetValue(id, out var pe) ? pe : (sEn.TryGetValue(id, out var se) ? se : null);
            if (en != null) idToEn[id] = en;

            if (pLoc.TryGetValue(id, out var pl)) idToLoc[id] = new(pl);
            if (sLoc.TryGetValue(id, out var sl))
            {
                if (!idToLoc.TryGetValue(id, out var cur)) { cur = new(StringComparer.Ordinal); idToLoc[id] = cur; }
                foreach (var kv in sl)
                    if (allowedLocales.Contains(kv.Key) && !cur.ContainsKey(kv.Key))
                        cur[kv.Key] = kv.Value;
            }
            if (idToLoc.TryGetValue(id, out var m))
                foreach (var k in m.Keys.ToList())
                    if (!allowedLocales.Contains(k))
                        m.Remove(k);
        }
        return (idToEn, idToLoc);
    }

    // --- Seed loader ---
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

    // --- Lua / CSV ---
    private static void WriteLua(string path, SortedDictionary<string, Dictionary<string, string>> data)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.WriteLine("-- Auto-generated by RebuildNpcNames (C# no-DB)\nRXPData = RXPData or {}\nRXPData.NPCNameDB = {");
        foreach (var kv in data)
        {
            var en = LuaEscape(kv.Key);
            var parts = string.Join(", ", kv.Value.OrderBy(p => p.Key, StringComparer.Ordinal)
                .Select(p => $"{p.Key}=\"{LuaEscape(p.Value)}\""));
            w.WriteLine($"  [\"{en}\"] = {{ {parts} }},");
        }
        w.WriteLine("}");
    }
    private static string LuaEscape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private static void WriteCsv(string path, string[] header, List<string[]> rows)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.WriteLine(string.Join(",", header));
        foreach (var r in rows) w.WriteLine(string.Join(",", r.Select(EscCsv)));
        static string EscCsv(string v) => (v.Contains('"') || v.Contains(',') || v.Contains('\n')) ? $"\"{v.Replace("\"", "\"\"")}\"" : v;
    }

    // --- Matching helpers ---
    private static string? FindBestMatch(string seed, IEnumerable<string> all, Dictionary<string, HashSet<string>> normIndex, int minFuzzy)
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
    private static int WRatio(string a, string b) { var r1 = Ratio(a, b); var r2 = Ratio(Norm(a), Norm(b)); return Math.Max(r1, r2); }
    private static int Ratio(string a, string b)
    { int lev = Levenshtein(a, b); int maxLen = Math.Max(1, Math.Max(a.Length, b.Length)); return (int)Math.Round((1.0 - (double)lev / maxLen) * 100); }
    private static int Levenshtein(string s, string t)
    {
        int n = s.Length, m = t.Length; var d = new int[n + 1, m + 1];
        for (int i = 0; i <= n; i++) d[i, 0] = i;
        for (int j = 0; j <= m; j++) d[0, j] = j;
        for (int i = 1; i <= n; i++)
            for (int j = 1; j <= m; j++)
            {
                int cost = s[i - 1] == t[j - 1] ? 0 : 1;
                d[i, j] = Math.Min(Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1), d[i - 1, j - 1] + cost);
            }
        return d[n, m];
    }
    private static string Norm(string s)
    {
        if (string.IsNullOrEmpty(s)) return s;
        s = s.Normalize(NormalizationForm.FormKC).Replace('’', '\'').Replace('–', '-').Replace('—', '-');
        s = Regex.Replace(s, "\\s*\\([^)]*\\)", "");
        s = Regex.Replace(s, "\\s+", " ").Trim();
        return s.ToLowerInvariant();
    }
    private static string MaybeSingular(string s)
    {
        var parts = s.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return s;
        var last = parts[^1];
        if (last.Length > 3 && last.EndsWith("s", StringComparison.Ordinal)) parts[^1] = last[..^1];
        return string.Join(' ', parts);
    }

    // --- simple args bag ---
    private sealed class ArgBag
    {
        private readonly Dictionary<string, List<string>> _map = new(StringComparer.OrdinalIgnoreCase);
        public void Set(string key, params string[] values) => _map[key] = values.ToList();
        public bool TryGetValue(string key, out string? value)
        {
            if (_map.TryGetValue(key, out var list) && list.Count > 0) { value = list[0]; return true; }
            value = null; return false;
        }
        public List<string>? TryGetValues(string key) => _map.TryGetValue(key, out var list) ? list : null;
    }
    private static ArgBag ParseArgs(string[] args)
    {
        var bag = new ArgBag();
        for (int i = 0; i < args.Length; i++)
        {
            var a = args[i];
            if (!a.StartsWith("--")) continue;
            var vals = new List<string>();
            int j = i + 1;
            while (j < args.Length && !args[j].StartsWith("--"))
            {
                vals.Add(args[j]); j++;
            }
            if (vals.Count == 0) bag.Set(a, "true"); else bag.Set(a, vals.ToArray());
            i = j - 1;
        }
        return bag;
    }
}
