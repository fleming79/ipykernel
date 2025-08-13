# Command line

`async-kernel` (and alias `async_kernel`) is provided as a system executable.

**Options:**

- [Start a kernel](#start-a-kernel)
- [Add kernel spec](#add-a-kernel-spec)
- [Remove](#remove-a-kernel-spec)

## Add a kernel spec

Use the argument `-a` followed by the kernel name to add a new kernel spec.
Include 'trio' in the kernel name to use a 'trio' backend. Any valid kernel name is
allowed do not include whitespace in the kernel name. Recommended kernel names are

- 'async': Default kernel that is installed that provides a the default 'asyncio' backend.
- 'async-trio': A trio backend. Note: trio must be installed separately.

Add a trio kernel spec.

```console
async-kernel -a async-trio
```

!!! note

```
To modify how the kernel start see [Start a kernel](#start-a-kernel) for configuration options.
```

### Configuration

Additional configuration of the kernel spec is supported by passing the each parameter
prefixed with '--' followed by the value.

The parameters are first used with creating the kernel spec.

## Remove a kernel spec

You can remove any kernel spec that is listed. Call `async-kernel` with no arguments to see a list of the installed kernels.

```shell
async-kernel
```

If you added the custom kernel spec above, you can remove it with:

```shell
async-kernel -r async-trio-custom
```

## Start a kernel

To start a kernel from the command prompt, use the argument `-f`.

This will start the default kernel (async).

```shell
async-kernel -f .
```

Additional settings can be passed as arguments.

```shell
async-kernel -f . --kernel_name async-trio-custom --display_name 'My custom kernel' --quiet False
```

The call above will start a new kernel with a 'trio' backend. The quiet setting is
a parameter that gets set on kernel. Parameters of this type are converted using [eval]
prior to setting.

For further detail, see the api for the command line handler [main]\[async_kernel.__main__.main\].
