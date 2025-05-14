import java.security.Security;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import java.lang.instrument.Instrumentation;

public class BouncyCastleAgent {
    public static void premain(String agentArgs, Instrumentation inst) {
        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
            System.out.println("BouncyCastle Provider Registered Successfully.");
        } else {
            System.out.println("BouncyCastle Provider Already Registered.");
        }
    }
}
