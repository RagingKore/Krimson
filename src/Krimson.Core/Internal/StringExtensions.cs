using static System.Text.RegularExpressions.Regex;

namespace Krimson;

static class StringExtensions {
    /// <summary>
    /// Separates the input words with underscore
    /// </summary>
    /// <param name="input">The string to be underscored</param>
    /// <returns></returns>
    public static string Underscore(this string input) =>
        Replace(Replace(Replace(input, @"([\p{Lu}]+)([\p{Lu}][\p{Ll}])", "$1_$2"), @"([\p{Ll}\d])([\p{Lu}])", "$1_$2"), @"[-\s]", "_").ToLower();

    /// <summary>
    /// Replaces underscores with dashes in the string
    /// </summary>
    public static string Dasherize(this string underscoredWord) => underscoredWord.Replace('_', '-');

    /// <summary>
    /// Separates the input words with hyphens and all the words are converted to lowercase
    /// </summary>
    public static string Kebaberize(this string input) => Underscore(input).Dasherize();
}