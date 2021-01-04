

class A {
    Script script;
    public void a() {
        script.println("Hello")
    }
}
def a = new A(script:this)
println( "Calling A.a()" )
a.a()


