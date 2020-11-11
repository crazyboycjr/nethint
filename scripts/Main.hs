module FlowSimulator where

newtype Simulator = Simulator 
    runSimulator :: Cluster -> Trace -> (Trace, Stats)
    runSimulator :: Cluster -> App -> (Trace, Stats)
    runSimulator :: Cluster -> ScheduledApp -> (Trace, Stats)
}
