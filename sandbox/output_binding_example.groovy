import hudson.model.*

// Get the out variable
def out = getBinding().out;

class OutputClass
{
    OutputClass(out)  // Have to pass the out variable to the class
    {
        out.println ("Inside class")
    }
}

out.println("Outside class")
output = new OutputClass(out)



//todo: interesting:
//https://stackoverflow.com/questions/7742472/groovy-script-in-jenkins-println-output-disappears-when-called-inside-class-envi

// todo: also:
// https://stackoverflow.com/questions/42149652/println-in-call-method-of-vars-foo-groovy-works-but-not-in-method-in-class