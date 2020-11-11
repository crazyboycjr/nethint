{-# LANGUAGE LambdaCase #-}

-- import Control.Monad.Trans.Except
import Control.Concurrent (threadDelay)
import Control.Exception
import Control.Monad
import Control.Monad.Loops (iterateWhile)
import Control.Monad.Extra (unlessM, whenM)
import Data.Char (isSpace)
import Data.Functor ((<&>))
import Data.List
import Data.Monoid
import System.Environment (getArgs)
import System.Exit
import System.FilePath
import System.Process
import Text.Printf (printf)


-- | This module prepares a set of helper functions and data
-- to help manipulate VMs in batch on different danyang servers simutaneously.

cpuVMsPerMachine, numMachines :: Int
cpuVMsPerMachine = 8
numMachines = 6

setRdmaIntfConfPath :: FilePath
setRdmaIntfConfPath =
    "/nfs/cjr/Developing/nethint-rs/scripts/testbed-2/meta_config/set_rdma_intf.conf.sh"

cpuVirtualMachineRdmaIntf = "ens3"

type Dom = String

checkArg :: [String -> Bool] -> String -> Any
checkArg = mconcat . map (Any .)

checkArgs :: [String -> Bool] -> IO Bool
checkArgs fs = getArgs <&> getAny . mconcat . map (checkArg fs)

checkArgsInList :: [String] -> IO Bool
checkArgsInList = checkArgs . map (==)

isDryRun :: IO Bool
isDryRun = checkArgsInList ["--dry-run", "-d"]

isStrict :: IO Bool
isStrict = checkArgsInList ["--strict", "-e"]

isDebug :: IO Bool
isDebug = not <$> checkArgsInList ["--no-debug"]

callCommandDebug :: String -> IO ()
callCommandDebug cmd = do
    whenM isDebug $ putStrLn $ "executing: " ++ cmd
    unlessM isDryRun $ callCommand cmd

readCommandWithExitCodeDebug :: String -> IO (ExitCode, String, String)
readCommandWithExitCodeDebug cmd = do
    whenM isDebug $ putStrLn $ "executing: " ++ cmd
    dryRun <- isDryRun
    if dryRun
        then error "dry run mode cannot actually execute a command and read the output"
        else readCreateProcessWithExitCode (shell cmd) ""


hostName :: IO String
hostName = dropWhileEnd isSpace <$> readProcess "hostname" [] ""
-- hostName = return "danyang-02"

hostId :: IO Int
hostId = read . tail . dropWhile (/= '-') <$> hostName

cpuVirtualMachines :: IO [String]
cpuVirtualMachines = do
    hostId' <- hostId
    let base = (hostId' - 1) * cpuVMsPerMachine
        ids  = [ base + i | i <- [0 .. cpuVMsPerMachine - 1] ]
    return $ map (("nixos" ++) . show) ids


sequenceRun_ :: (a -> IO ()) -> [a] -> IO ()
sequenceRun_ f []       = return ()
sequenceRun_ f (x : xs) = do
    catch (f x) $ \e -> do
        let errmsg = "*** Exception: " ++ show (e :: SomeException)
        isStrict >>= \case
            True  -> die errmsg
            False -> putStrLn errmsg
    sequenceRun_ f xs

listVirtualMachines :: IO ()
listVirtualMachines = callCommandDebug "virsh list --all"

virshDomCommand :: String -> Dom -> IO ()
virshDomCommand subcmd = callCommandDebug . printf "virsh %s %s" subcmd

startVirtualMachine, shutdownVirtualMachine, destroyVirtualMachine, undefineVirtualMachine
    :: Dom -> IO ()
startVirtualMachine = virshDomCommand "start"
shutdownVirtualMachine = virshDomCommand "shutdown"
destroyVirtualMachine = virshDomCommand "destroy"
undefineVirtualMachine = virshDomCommand "undefine"

startVirtualMachineAll, shutdownVirtualMachineAll, destroyVirtualMachineAll, undefineVirtualMachineAll
    :: [Dom] -> IO ()
startVirtualMachineAll = sequenceRun_ startVirtualMachine
shutdownVirtualMachineAll = sequenceRun_ shutdownVirtualMachine
destroyVirtualMachineAll = sequenceRun_ destroyVirtualMachine
undefineVirtualMachineAll = sequenceRun_ undefineVirtualMachine

cpuRdmaNetMask :: String
cpuRdmaNetMask = "/24"

cpuRdmaIpAddr :: IO [String]
cpuRdmaIpAddr = do
    hostId' <- hostId
    let base     = if hostId' <= 3 then (hostId' - 1) * 32 + 3 else hostId' * 32 + 3
        suffixes = [ base + i | i <- [0 .. cpuVMsPerMachine - 1] ]
    return $ map (("192.168.211." ++) . show) suffixes

cpuRdmaCIDR :: IO [String]
cpuRdmaCIDR = map (++ cpuRdmaNetMask) <$> cpuRdmaIpAddr


isAlive :: String -> IO Bool
isAlive dom = do
    (rc, out, err) <- readCommandWithExitCodeDebug $ "virsh domstate " ++ dom
    case rc of
        ExitSuccess      -> return $ (== "running") $ dropWhileEnd isSpace out
        ExitFailure code -> die $ printf "stdout: %s\nstderr: %s\n" out err

getDomIfAddr :: Dom -> IO String
getDomIfAddr dom = do
    (rc, out, err) <- readCommandWithExitCodeDebug
        $ printf "virsh domifaddr %s | grep ipv4 | awk '{print $4}' | cut -d'/' -f1" dom
    case rc of
        ExitSuccess      -> return $ dropWhileEnd isSpace out
        ExitFailure code -> die $ printf "stdout: %s\nstderr: %s\n" out err

waitDomIfAddr :: Dom -> IO String
waitDomIfAddr dom = do
    iterateWhile null $ do
        threadDelay 1000000 -- sleep for 1s
        printf "Querying IP address of %s...\n" dom
        getDomIfAddr dom

waitDomOnline :: Dom -> IO ()
waitDomOnline = void . waitDomIfAddr

waitDomOnlineAll :: [Dom] -> IO ()
waitDomOnlineAll = mapM_ waitDomOnline

deployRdmaConfig :: Dom -> String -> IO ()
deployRdmaConfig dom ipCIDR = do
    -- call set_rdma_intf.conf.sh <cidr> > /tmp/set_rdma_intf.sh
    -- call scp /tmp/set_rdma_intf.sh tenant@nixos0: # how to get this IP
    -- call ssh tenant@nixos0 'sudo ~/set_rdma_intf.sh'
    let confScript :: String
        confScript = printf "/tmp/set_rdma_intf_%s.sh" dom
        cmd1       = printf "%s %s > %s" setRdmaIntfConfPath ipCIDR confScript
    callCommandDebug cmd1
    scp dom confScript "~/set_rdma_intf.sh"
    sshAndExecute dom
        $  "chmod +x ~/set_rdma_intf.sh && sudo ~/set_rdma_intf.sh "
        ++ cpuVirtualMachineRdmaIntf

deployRdmaConfigAll :: [Dom] -> [String] -> IO ()
deployRdmaConfigAll doms ipCIDRs = sequenceRun_ (uncurry deployRdmaConfig) (zip doms ipCIDRs)

scp :: Dom -> FilePath -> FilePath -> IO ()
scp dom src dst = do
    ipAddr <- getDomIfAddr dom
    let scpCmd = printf "scp -oStrictHostKeyChecking=no %s tenant@%s:%s" src ipAddr dst
    callCommandDebug scpCmd

scpAll :: [Dom] -> FilePath -> FilePath -> IO ()
scpAll doms src dst = sequenceRun_ (\d -> scp d src dst) doms

sshAndExecute :: Dom -> String -> IO ()
sshAndExecute dom cmd = do
    ipAddr <- getDomIfAddr dom
    let sshCmd = printf "ssh -oStrictHostKeyChecking=no tenant@%s '%s'" ipAddr cmd
    callCommandDebug sshCmd

sshAndExecuteAll :: [Dom] -> String -> IO ()
sshAndExecuteAll doms cmd = sequenceRun_ (`sshAndExecute` cmd) doms

ssh :: Dom -> IO ()
ssh dom = do
    ipAddr <- getDomIfAddr dom
    let sshCmd = printf "ssh -oStrictHostKeyChecking=no tenant@%s" ipAddr
    callCommandDebug sshCmd

oneClickSetup :: Int -> IO ()
oneClickSetup n = do
    cpus <- take n <$> cpuVirtualMachines
    rdmaAddrs <- take n <$> cpuRdmaCIDR
    startVirtualMachineAll cpus
    waitDomOnlineAll cpus
    deployRdmaConfigAll cpus rdmaAddrs
    sshAndExecuteAll cpus "ip a"

psrecord :: String -> Double -> Double -> IO ()
psrecord name interval duration = do
    let
        cmd = printf
            "psrecord $(pgrep %s | head -n 1) --interval %.f --duration %.f --plot /tmp/%s.png --include-children --log /tmp/%s.txt"
            name
            interval
            duration
            name
            name
    callCommand cmd

{-
import Control.Concurrent.Async (mapConcurrently)

:{

myPkill = do
    sshAndExecuteAll cpus "pkill -f /tmp/worker"
    sshAndExecuteAll cpus "pkill -f controller"

myScp = do
    let prefix = "/nfs/cjr/Developing/nethint-rs/target/release/"
        targets = ["scheduler", "rplaunch", "controller", "worker"]
    void $ mapConcurrently (flip (scpAll cpus) "/tmp/") $ map (prefix ++) targets
:}

:m -Control.Concurrent.Async
-}
