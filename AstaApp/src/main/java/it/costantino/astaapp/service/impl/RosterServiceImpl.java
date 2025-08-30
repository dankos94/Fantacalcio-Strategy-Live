package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Roster;
import it.costantino.astaapp.repository.RosterRepository;
import it.costantino.astaapp.service.RosterService;
import it.costantino.astaapp.service.dto.RosterDTO;
import it.costantino.astaapp.service.mapper.RosterMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Roster}.
 */
@Service
@Transactional
public class RosterServiceImpl implements RosterService {

    private static final Logger LOG = LoggerFactory.getLogger(RosterServiceImpl.class);

    private final RosterRepository rosterRepository;

    private final RosterMapper rosterMapper;

    public RosterServiceImpl(RosterRepository rosterRepository, RosterMapper rosterMapper) {
        this.rosterRepository = rosterRepository;
        this.rosterMapper = rosterMapper;
    }

    @Override
    public RosterDTO save(RosterDTO rosterDTO) {
        LOG.debug("Request to save Roster : {}", rosterDTO);
        Roster roster = rosterMapper.toEntity(rosterDTO);
        roster = rosterRepository.save(roster);
        return rosterMapper.toDto(roster);
    }

    @Override
    public RosterDTO update(RosterDTO rosterDTO) {
        LOG.debug("Request to update Roster : {}", rosterDTO);
        Roster roster = rosterMapper.toEntity(rosterDTO);
        roster = rosterRepository.save(roster);
        return rosterMapper.toDto(roster);
    }

    @Override
    public Optional<RosterDTO> partialUpdate(RosterDTO rosterDTO) {
        LOG.debug("Request to partially update Roster : {}", rosterDTO);

        return rosterRepository
            .findById(rosterDTO.getId())
            .map(existingRoster -> {
                rosterMapper.partialUpdate(existingRoster, rosterDTO);

                return existingRoster;
            })
            .map(rosterRepository::save)
            .map(rosterMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<RosterDTO> findAll() {
        LOG.debug("Request to get all Rosters");
        return rosterRepository.findAll().stream().map(rosterMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<RosterDTO> findOne(Long id) {
        LOG.debug("Request to get Roster : {}", id);
        return rosterRepository.findById(id).map(rosterMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Roster : {}", id);
        rosterRepository.deleteById(id);
    }
}
