/*** convenient "-flag opt" combination ***/

 import java.util.*;

class OptElement {
     String flag, opt;
     public OptElement(String flag, String opt) { this.flag = flag; this.opt = opt; }
}

class Options {
    public ArrayList<String> argsList = new ArrayList<>();  
    public ArrayList<OptElement> optsList = new ArrayList<>();
    public ArrayList<String> doubleOptsList = new ArrayList<>();
    
    public void Parse (String[] args)
    {
        for (int i = 0; i < args.length; i++) {
            switch (args[i].charAt(0)) {
            case '-':
                if (args[i].length() < 2)
                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
                if (args[i].charAt(1) == '-') {
                    if (args[i].length() < 3)
                        throw new IllegalArgumentException("Not a valid argument: "+args[i]);
                    // --opt
                    doubleOptsList.add(args[i].substring(2, args[i].length()));
                }
                  else {
                    if (args.length-1 == i)
                        throw new IllegalArgumentException("Expected arg after: "+args[i]);
                    // -opt
                    optsList.add(new OptElement(args[i], args[i+1]));
                    i++;
                }
                break;
            default:
                // arg
                argsList.add(args[i]);
                break;
            }
        }
        // etc
    }
    public void PrintOpts() {
        System.out.println("argsList = " + argsList);
        System.out.println("doubleOptsList = " + doubleOptsList);
        for (OptElement x: optsList) {
            System.out.println("flag = " + x.flag + " opt = " + x.opt);
        }
    }
    public OptElement GetElement(int pos) {
        if(pos <= optsList.size())
            return optsList.get(pos);
        else
            return null;
        }
}
