# NullAway Configuration for Bazel

NullAway is a static analysis tool that helps eliminate NullPointerExceptions (NPEs) by detecting potential null pointer dereferences at compile time.

## Current Status

⚠️ **NullAway dependency needs to be added to maven_install.json**

The NullAway configuration is set up in this repository, but the dependency needs to be added to the Maven lock file before it can be used.

## Setup Instructions

### Step 1: Update maven_install.json

Since `fail_if_repin_required` is already set to `False` in MODULE.bazel, you can proceed directly to building:

```bash
# This will fetch NullAway and update the unpinned dependencies
bazel build //tools/nullaway:nullaway
```

If you encounter Maven resolution issues, the dependency should download automatically when `fail_if_repin_required = False`.

### Step 2: Update Lock File (When Pin Command Works)

When the Maven pin command is fixed, run:

```bash
REPIN=1 bazel run @buildfarm_maven//:pin
```

This will update `maven_install.json` with the NullAway dependency.

## Using NullAway

### Option 1: Run with --config=nullaway (Recommended)

Enable NullAway for all targets in your build:

```bash
# Build with NullAway analysis
bazel build --config=nullaway //...

# Test with NullAway analysis
bazel test --config=nullaway //...
```

### Option 2: Add to Specific Targets

Add the NullAway plugin to specific `java_library` or `java_binary` targets:

```python
java_library(
    name = "my_library",
    srcs = ["MyClass.java"],
    plugins = ["//tools/nullaway"],
    deps = [...],
)
```

## Configuration

The NullAway configuration is defined in `.bazelrc`:

```
build:nullaway --javacopt=-XepDisableWarningsInGeneratedCode
build:nullaway --javacopt=-Xep:NullAway:ERROR
build:nullaway --javacopt=-XepOpt:NullAway:AnnotatedPackages=build.buildfarm
build:nullaway --javacopt=-XepOpt:NullAway:TreatGeneratedAsUnannotated=true
build:nullaway --javacopt=-XepOpt:NullAway:CheckOptionalEmptiness=true
build:nullaway --javacopt=-XepOpt:NullAway:SuggestSuppressions=true
```

### Configuration Options

- **AnnotatedPackages**: Specifies which packages to analyze (set to `build.buildfarm`)
- **TreatGeneratedAsUnannotated**: Skip analysis of generated code
- **CheckOptionalEmptiness**: Verify proper usage of `Optional.empty()`
- **SuggestSuppressions**: Provide `@SuppressWarnings` suggestions for false positives

## Fixing Null Issues

When NullAway reports potential NPEs, you can fix them by:

1. **Adding null checks**:
   ```java
   if (obj != null) {
       obj.method();
   }
   ```

2. **Using @Nullable annotations**:
   ```java
   import org.jspecify.annotations.Nullable;

   public void method(@Nullable String param) {
       if (param != null) {
           // use param
       }
   }
   ```

3. **Using @NonNull annotations** (for clarity):
   ```java
   import javax.annotation.Nonnull;

   public void method(@Nonnull String param) {
       // param is guaranteed non-null
   }
   ```

4. **Suppressing false positives** (use sparingly):
   ```java
   @SuppressWarnings("NullAway")
   public void method() {
       // ...
   }
   ```

## Troubleshooting

### Maven Pin Command Fails

If `REPIN=1 bazel run @buildfarm_maven//:pin` fails with errors about `java_binary` attributes, this is a known compatibility issue with certain versions of `rules_jvm_external`.

Workarounds:
1. Keep `fail_if_repin_required = False` in MODULE.bazel (already set)
2. Use unpinned dependencies (Bazel will fetch them automatically)
3. Wait for rules_jvm_external compatibility fixes

### NullAway Target Not Found

If you get an error like `no such target '@buildfarm_maven//:com_uber_nullaway_nullaway'`, it means the dependency hasn't been downloaded yet. With `fail_if_repin_required = False`, try:

```bash
bazel clean --expunge
bazel build //tools/nullaway:nullaway
```

## Resources

- [NullAway GitHub](https://github.com/uber/NullAway)
- [NullAway Configuration Guide](https://github.com/uber/NullAway/wiki/Configuration)
- [Error Prone Documentation](https://errorprone.info/)
