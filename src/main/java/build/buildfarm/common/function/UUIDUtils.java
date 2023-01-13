package build.buildfarm.common.function;
import com.fasterxml.uuid.Generators;
import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

public class UUIDUtils {
    protected static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = ThreadLocal.withInitial(SecureRandom::new);
    public static UUID randomUUID() {
        return Generators.randomBasedGenerator(THREAD_LOCAL_RANDOM.get()).generate();
    }
}
