var target = Argument("target", "Default");

// adjust the current directory since cake is weird about this.
// alternatively we could figure how to launch PS in with the 
// correct working directory
Environment.CurrentDirectory = System.IO.Path.GetFullPath(@"..\..");

var solution = "./EsentQueue.sln";

// Restores dependencies
Task("NuGet-Restore")
    .Does(() =>
{
    NuGetRestore(solution);
});

Task("Build")
    .IsDependentOn("NuGet-Restore")
    .Does(() =>
{
    MSBuild(solution);
});

Task("Default")
    .IsDependentOn("Build");

RunTarget(target);