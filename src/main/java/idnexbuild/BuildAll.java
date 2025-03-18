package idnexbuild;



public class BuildAll {
    public static void main(String[] args) throws Exception {
        QPIndexBuild.QPIndexBuild(args);
        KPIndexBuild.KPIndexBuild(args);
        RPIndexBuild.RPIndexBuild(args);
        FPIndexBuild.FPIndexBuild(args);
        APIndexBuild.APIndexBuild(args);
    }
}
