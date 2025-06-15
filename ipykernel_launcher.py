"""Entry point for launching an IPython kernel."""

if __name__ == "__main__":
    import sys

    from ipykernel.kernelapp import MainKernel

    # TODO: specify the backend from sys.argv
    sys.argv

    MainKernel.start()
