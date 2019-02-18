package build.buildfarm.common;

import java.io.IOException;

public class IOUtils {
  private IOUtils() {
  }

  enum IOErrorFormatter {
    AccessDeniedException("access denied"),
    FileSystemException(""),
    IOException(""),
    NoSuchFileException("no such file");

    private final String description;

    IOErrorFormatter(String description) {
      this.description = description;
    }

    String toString(IOException e) {
      if (description.isEmpty()) {
        return e.getMessage();
      }
      return String.format("%s: %s", e.getMessage(), description);
    }
  }

  public static String formatIOError(IOException e) {
    IOErrorFormatter formatter;
    try {
      formatter = IOErrorFormatter.valueOf(e.getClass().getSimpleName());
    } catch (IllegalArgumentException eUnknown) {
      formatter = IOErrorFormatter.valueOf("IOException");
    }
    return formatter.toString(e);
  }
}
