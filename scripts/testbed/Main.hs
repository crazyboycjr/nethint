{-# LANGUAGE LambdaCase #-}

-- import Control.Monad.Trans.Except
import Control.Exception
import Control.Monad
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

virtualFunctionXmlPath :: FilePath
virtualFunctionXmlPath = "/nfs/cjr/Developing/nethint-rs/scripts/testbed/vfconfig"

setRdmaIntfConfPath :: FilePath
setRdmaIntfConfPath =
    "/nfs/cjr/Developing/nethint-rs/scripts/testbed/environment/meta_config/set_rdma_intf.conf.sh"

cpuVirtualMachineRdmaIntf = "enp6s0"

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
    return $ map (("cpu" ++) . show) ids

attachVirtualFunction
    :: Dom -- ^ DOM name in libvirt
    -> Int -- ^ the ID of the virtual function to attach
    -> IO ()
attachVirtualFunction dom vfid =
    let
        vfconfig = virtualFunctionXmlPath </> ("vf" ++ show vfid ++ ".xml")
        cmd      = "virsh attach-device --domain " ++ dom ++ " --file " ++ vfconfig
    in callCommandDebug cmd

attachVirtualFunctionAll :: [Dom] -> [Int] -> IO ()
attachVirtualFunctionAll doms vfids = forM_ (zip doms vfids) $ uncurry attachVirtualFunction

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

deployRdmaConfig :: Dom -> String -> IO ()
deployRdmaConfig dom ipCIDR = do
    -- call set_rdma_intf.conf.sh <cidr> > /tmp/set_rdma_intf.sh
    -- call scp /tmp/set_rdma_intf.sh tenant@cpu0: # how to get this IP
    -- call ssh tenant@cpu0 'sudo ~/set_rdma_intf.sh'
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
